#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Entry point for flask web server implementin Wikimedia Metrics API.

    Job Queue and Processing
    ^^^^^^^^^^^^^^^^^^^^^^^^

    As requests are issued via http to the API a process queue will store all active jobs. Processes will be created
    and assume one of the following states throughout their existence: ::

        * 'pending' - The request has yet to be fully processed and exposed but is underway
        * 'success' - The request has finished processing and is exposed at the url
        * 'failure' - The result has finished processing but dailed to expose results

    When a process a request is received and a job is created to service that request it enters the 'pending' state.
    If the job returns without exception it enters the 'success' state, otherwise it enters the 'failure' state.  The
    jpb remains in either of these states until it is cleared from the process queue.

    Response Data
    ^^^^^^^^^^^^^

    As requests are made to the API the data generated and formatted as JSON.  The definition of is as follows: ::

        {   header : header_list,
            cohort_expr : cohort_gen_timestamp : metric : timeseries : aggregator : date_start :
                date_end : [ metric_param : ]* : data
        }

    Where each component is defined: ::

        header_str := list(str), list of header values
        cohort_expr := str, cohort ID expression
        cohort_gen_timestamp := str, cohort generation timestamp (earliest of all cohorts in expression)
        metric := str, user metric handle
        timeseries := boolean, indicates if this is a timeseries
        aggregator := str, aggregator used
        date_start := str, start datetime of request
        date_end := str, end datetime of request
        metric_param := -, optional metric parameters
        data := list(tuple), set of data points

    Request data is mapped to a query via metric objects and hashed in the dictionary `pkl_data`.

    Cohort Data
    ^^^^^^^^^^^

    Cohort data is maintained in the host s1-analytics-slave.eqiad.wmnet under the `staging` database in the `usertags`
    and `usertags_meta` tables: ::

        +---------+-----------------+------+-----+---------+-------+
        | Field   | Type            | Null | Key | Default | Extra |
        +---------+-----------------+------+-----+---------+-------+
        | ut_user | int(5) unsigned | NO   | PRI | NULL    |       |
        | ut_tag  | int(4) unsigned | NO   | PRI | NULL    |       |
        +---------+-----------------+------+-----+---------+-------+

        +-------------+-----------------+------+-----+---------+----------------+
        | Field       | Type            | Null | Key | Default | Extra          |
        +-------------+-----------------+------+-----+---------+----------------+
        | utm_id      | int(5) unsigned | NO   | PRI | NULL    | auto_increment |
        | utm_name    | varchar(255)    | NO   |     |         |                |
        | utm_notes   | varchar(255)    | YES  |     | NULL    |                |
        | utm_touched | datetime        | YES  |     | NULL    |                |
        +-------------+-----------------+------+-----+---------+----------------+

"""

from flask import Flask, render_template, Markup, jsonify, \
    redirect, url_for, make_response, request, escape

import cPickle
from re import search
from config import logging
import os
import json
from src.utils.record_type import *
import config.settings as settings
import multiprocessing as mp
import collections
from dateutil.parser import parse as date_parse
from datetime import timedelta, datetime
from collections import OrderedDict

import src.etl.data_loader as dl
import src.metrics.metrics_manager as mm
from src.api import COHORT_REGEX, parse_cohorts, MetricsAPIError

######
#
# Define Globals and Data Types
#
#######

app = Flask(__name__)

# hash for jobs in queue_data dict
global global_id
global_id = 0

# Stores cached requests (this should eventually be replaced with a proper cache)
global pkl_data
pkl_data = OrderedDict()

# stores data in Queue objects that are active
global queue_data
queue_data = dict()

# Error codes for web requests
global error_codes
error_codes = {
    0 : 'Job already running.',
    1 : 'Badly Formatted timestamp',
}

# Queue for storing all active processes
global processQ
processQ = list()

# Class defining all objects contained on the processQ
QStructClass = collections.namedtuple('QStruct', 'id process request url queue status')

# Define the standard variable names in the query string - store in named tuple
RequestMeta = recordtype('RequestMeta', 'cohort_expr cohort_gen_timestamp metric time_series ' + \
                                                           'aggregator date_start date_end interval t n')

def RequestMetaFactory(cohort_expr, cohort_gen_timestamp, metric, time_series, aggregator, date_start, date_end,
                       interval=None, t=None, n=None):
    return RequestMeta(cohort_expr, cohort_gen_timestamp, metric, time_series, aggregator, date_start, date_end,
        interval, t, n)

REQUEST_META_QUERY_STR = ['aggregator', 'time_series', 'date_start', 'date_end', 'interval', 't', 'n']
REQUEST_META_BASE = ['cohort_expr', 'metric']

# Datetime string format to be used throughout the API
DATETIME_STR_FORMAT = "%Y%m%d%H%M%S"

######
#
# Define Decorators and helper methods
#
#######

def get_errors(request_args):
    error = ''
    if 'error' in request_args:
        try:
            error = error_codes[int(request_args['error'])]
        except KeyError: pass
        except ValueError: pass
    return error

def process_request_params(request_meta):
    """
        Applies defaults and consistency to RequestMeta data

            request_meta - RequestMeta recordtype.  Stores the request data.
    """

    DEFAULT_INTERVAL = 1
    TIME_STR = '000000'

    end = datetime.now()
    start= end + timedelta(days=-DEFAULT_INTERVAL)

    # Handle any datetime fields passed - raise an exception if the formatting is incorrect
    if request_meta.date_start:
        try:
            request_meta.date_start = date_parse(request_meta.date_start).strftime(DATETIME_STR_FORMAT)[:8] + TIME_STR
        except ValueError:
            raise MetricsAPIError('1') # Pass the value of the error code in `error_codes`
    else:
        request_meta.date_start = start.strftime(DATETIME_STR_FORMAT)[:8] + TIME_STR

    if request_meta.date_end:
        try:
            request_meta.date_end = date_parse(request_meta.date_end[:8]).strftime(DATETIME_STR_FORMAT) + TIME_STR
        except ValueError:
            raise MetricsAPIError('1') # Pass the value of the error code in `error_codes`
    else:
        request_meta.date_end = end.strftime(DATETIME_STR_FORMAT)[:8] + TIME_STR

    request_meta.time_series = True if request_meta.time_series else None

    agg_key = mm.get_agg_key(request_meta.metric, request_meta.aggregator)
    request_meta.aggregator = agg_key if agg_key else None

def process_metrics(p, rm):
    """ Worker process for requests - this will typically operate in a forked process """

    conn = dl.Connector(instance='slave')
    logging.info(__name__ + '::START JOB %s (PID = %s)' % (str(rm), os.getpid()))

    # process metrics
    users = get_users(rm.cohort_expr)
    args = { attr : getattr(rm, attr) for attr in REQUEST_META_QUERY_STR} # unpack RequestMeta into dict
    results = mm.process_data_request(rm.metric, users, **args)

    p.put(jsonify(results))
    del conn
    logging.info(__name__ + '::END JOB %s (PID = %s)' % (str(rm), os.getpid()))

def get_users(cohort_expr):
    """ get users from cohort """

    if search(COHORT_REGEX, cohort_expr):
        logging.info(__name__ + '::Processing cohort by expression.')
        users = [user for user in parse_cohorts(cohort_expr)]
    else:
        logging.info(__name__ + '::Processing cohort by tag name.')
        conn = dl.Connector(instance='slave')
        try:
            conn._cur_.execute('select utm_id from usertags_meta where utm_name = "%s"' % str(cohort_expr))
            res = conn._cur_.fetchone()[0]
            conn._cur_.execute('select ut_user from usertags where ut_tag = "%s"' % res)
        except IndexError:
            redirect(url_for('cohorts'))
        users = [r[0] for r in conn._cur_]
        del conn
    return users

def get_cohort_id(utm_name):
    """ Pull cohort ids from cohort handles """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('SELECT utm_id FROM usertags_meta WHERE utm_name = "%s"' % str(escape(utm_name)))

    utm_id = None
    try: utm_id = conn._cur_.fetchone()[0]
    except ValueError: pass

    # Ensure the field was retrieved
    if not utm_id:
        logging.error(__name__ + '::Missing utm_id for cohort %s.' % str(utm_name))
        utm_id = -1

    del conn
    return utm_id

def get_cohort_refresh_datetime(utm_id):
    """ Get the latest refresh datetime of a cohort.  Returns current time formatted as a
     string if the field is not found. """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('SELECT utm_touched FROM usertags_meta WHERE utm_id = %s' % str(escape(utm_id)))

    utm_touched = None
    try: utm_touched = conn._cur_.fetchone()[0]
    except ValueError: pass

    # Ensure the field was retrieved
    if not utm_touched:
        logging.error(__name__ + '::Missing utm_touched for cohort %s.' % str(utm_id))
        utm_touched = datetime.now()

    del conn
    return utm_touched.strftime(DATETIME_STR_FORMAT)

def get_data(request_meta):
    """ Extract data from the global hash given a request object """

    global pkl_data

    # Traverse the hash key structure to find data
    data_ref = pkl_data
    for key_name in REQUEST_META_BASE + REQUEST_META_QUERY_STR:
        key = getattr(request_meta,key_name)
        if not key: continue  # Only process keys that have been set
        if hasattr(data_ref, 'has_key') and data_ref.has_key(key):
            data_ref = data_ref[key]
        else:
            return None

    # Ensure that an interface that does not rely on keyed values is returned
    # all data must be in interfaces resembling lists
    if not hasattr(data_ref, '__iter__'):
        return data_ref
    else:
        return None

def set_data(request_meta, data):
    """ Given request meta-data and a dataset create a key path in the global hash to store the data """

    global pkl_data

    key_sig = list()

    # Build the key signature
    for key_name in REQUEST_META_BASE: # These keys must exist
        key = getattr(request_meta, key_name)
        if key:
            key_sig.append(key)
        else:
            logging.error(__name__ + '::Request must include %s. Cannot set data %s.' % (key_name, str(request_meta)))
            return

    for key_name in REQUEST_META_QUERY_STR: # These keys may optionally exist
        key = getattr(request_meta, key_name)
        if key: key_sig.append(key)

    # For each key in the key signature add a nested key to the hash
    pkl_data_ref = pkl_data
    last_item = key_sig[len(key_sig) - 1]
    for key in key_sig:
        if key != last_item:
            if not (hasattr(pkl_data_ref, 'has_key') and pkl_data_ref.has_key(key)):
                pkl_data_ref[key] = OrderedDict()
            pkl_data_ref = pkl_data_ref[key]
        else:
            pkl_data_ref[key] = data


######
#
# Define View methods
#
#######

@app.route('/')
def api_root():
    """ View for root url - API instructions """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn

    return render_template('index.html', cohort_data=data)

@app.route('/tag_definitions')
def tag_definitions():
    """ View for tag definitions where cohort meta dat can be reviewed """

    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select * from usertags_meta')

    f = dl.DataLoader().cast_elems_to_string
    usertag_meta_data = [escape(", ".join(f(r))) for r in conn._cur_]

    del conn
    return render_template('tag_definitions.html', data=usertag_meta_data)

@app.route('/cohorts')
def cohorts():
    """ View for listing and selecting cohorts """

    error = get_errors(request.args)

    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select distinct utm_name from usertags_meta')
    o = [r[0] for r in conn._cur_]

    del conn
    return render_template('cohorts.html', data=o, error=error)

@app.route('/metrics')
@app.route('/metrics/<string:cohort>')
def metrics(cohort=''):
    """ View for listing and selecting metrics """
    if not cohort:
        return redirect(url_for('cohorts'))
    else:
        return render_template('metrics.html', c_str=cohort, m_list=mm.get_metric_names())

@app.route('/metrics/<string:cohort>/<string:metric>')
def output(cohort, metric):
    """ View corresponding to a data request - all of the setup and execution for a request happens here. """

    global global_id
    url = request.url.split(request.url_root)[1]

    # Check for refresh flag
    refresh = True if 'refresh' in request.args else False

    cid = get_cohort_id(cohort)
    cohort_refresh_ts = get_cohort_refresh_datetime(cid)
    rm = RequestMetaFactory(cohort, cohort_refresh_ts, metric, None, None, None, None)
    for param in REQUEST_META_QUERY_STR:
        if param in request.args: setattr(rm, param, request.args[param])

    try:
        process_request_params(rm)
    except MetricsAPIError as e:
        return redirect(url_for('cohorts') + '?error=' + e.message)

    # Determine if the request maps to an existing repsonse.  If so return it.  Otherwise compute.
    data = get_data(rm)
    if data and not refresh:
        return data
    else:

        # Ensure that the job for this url is not already running
        is_pending_job = False
        for p in processQ:
            if not cmp(rm, p.request) and p.status[0] == 'pending': is_pending_job = True

        if not is_pending_job: # Queue the job

            q = mp.Queue()
            p = mp.Process(target=process_metrics, args=(q, rm))
            p.start()

            global_id += 1

            logging.info(__name__ + '::Appending request %s to the queue...' % rm)
            processQ.append(QStructClass(global_id,p,rm,url,q,['pending']))

            return render_template('processing.html', url_str=str(rm))
        else:
            return redirect(url_for('job_queue') + '?error=0')

@app.route('/job_queue')
def job_queue():
    """ View for listing current jobs working """

    error = get_errors(request.args)

    p_list = list()
    p_list.append(Markup('<u><b>is_alive , PID, url, status</b></u><br>'))
    for p in processQ:
        try:

            # Pull data off of the queue and add it to the queue data
            while not p.queue.empty():
                if not queue_data.has_key(p.id):
                    queue_data[p.id] = json.loads(p.queue.get().data)
                else:
                    for k,v in queue_data[p.id]:
                        if hasattr(v,'__iter__'): queue_data[p.id][k].extend(v)

            # once a process has finished working remove it and put its contents into the cache
            if not p.process.is_alive() and p.status[0] == 'pending':
                q_response = make_response(jsonify(queue_data[p.id]))
                del queue_data[p.id]
                set_data(p.request, q_response)

                p.status[0] = 'success'
                logging.info(__name__ + '::Completed request %s.' % p.url)

        except Exception as e:
            p.status[0] = 'failure'
            logging.error(__name__ + "::Could not update request: %s.  Exception: %s" % (p.url, e.message) )

        # Log the status of the job
        response_url = "".join(['<a href="', request.url_root, p.url + '">', p.url, '</a>'])
        p_list.append(" , ".join([str(p.process.is_alive()), str(p.process.pid),
                                  escape(Markup(response_url)), p.status[0]]))

    if error:
        return render_template('queue.html', procs=p_list, error=error)
    else:
        return render_template('queue.html', procs=p_list)

@app.route('/all_urls')
def all_urls():
    """ View for listing all requests """

    url_list = list()
    for url in pkl_data.keys():
        url_list.append("".join(['<a href="', request.url_root, url + '">', url, '</a>']))
    return render_template('all_urls.html', urls=url_list)


######
#
# Define Custom Classes
#
#######

class APIMethods(object):
    """ Provides initialization and boilerplate for API execution """

    __instance = None   # Singleton instance

    def __new__(cls):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(APIMethods, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        """ Load cached data from pickle file. """
        global pkl_data

        # Open the pickled data for reading.
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'rb')
        except IOError:
            pkl_file = None

        # test whether the open was successful
        if pkl_file:
            pkl_data = cPickle.load(pkl_file)
            pkl_file.close()

    def __del__(self):
        """  When the instance is deleted store the pickled data """
        global pkl_data

        pkl_file = None
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'wb')
            cPickle.dump(pkl_data, pkl_file)
        except Exception:
            logging.error(__name__ + '::Could not pickle data.')
        finally:
            if hasattr(pkl_file, 'close'): pkl_file.close()


######
#
# Execution
#
#######

if __name__ == '__main__':

    a = APIMethods() # initialize API data - get the instance
    try:
        app.run()
    finally:
        del a



