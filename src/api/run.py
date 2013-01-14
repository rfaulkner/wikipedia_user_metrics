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
from config import logging
import os
import json
import config.settings as settings
import multiprocessing as mp
import collections
from collections import OrderedDict

import src.etl.data_loader as dl
import src.metrics.metrics_manager as mm
from src.api import MetricsAPIError

from engine import *

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

def build_url_from_request(request): pass


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

@app.route('/about/')
def about():
    return render_template('about.html')

@app.route('/contact/')
def contact():
    return render_template('contact.html')

@app.route('/tags/')
def tags():
    """ View for tag definitions where cohort meta data can be reviewed """

    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select * from usertags_meta')

    f = dl.DataLoader().cast_elems_to_string
    utm = [escape(", ".join(f(r))) for r in conn._cur_]

    del conn
    return render_template('tags.html', data=utm)

@app.route('/metrics/', methods=['POST', 'GET'])
def all_metrics():
    """ Display a list of available metrics """
    if request.method == 'POST':
        #@@@ TODO  validate form input against existing cohorts
        return metric(request.form['selectMetric'])
    else:
        return render_template('all_metrics.html')

@app.route('/metrics/<string:metric>')
def metric(metric=''):
    """ Display single metric documentation """
    #@@@ TODO validate metric against existing metrics
    return render_template('metric.html', m_str=metric)

@app.route('/cohorts/', methods=['POST', 'GET'])
def all_cohorts():
    """ View for listing and selecting cohorts """
    error = get_errors(request.args)

    if request.method == 'POST':
        #@@@ TODO  validate form input against existing cohorts
        return cohort(request.form['selectCohort'])
    else:
        conn = dl.Connector(instance='slave')
        conn._cur_.execute('select distinct utm_name from usertags_meta')
        o = [r[0] for r in conn._cur_]
        del conn
        return render_template('all_cohorts.html', data=o)

@app.route('/cohorts/<string:cohort>')
def cohort(cohort=''):
    """ View single cohort page """
    if not cohort:
        return redirect(url_for('all_cohorts'))
    else:
        return render_template('cohort.html', c_str=cohort, m_list=mm.get_metric_names())

@app.route('/cohorts/<string:cohort>/<string:metric>')
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
    data = get_data(rm, pkl_data)
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

@app.route('/job_queue/')
def job_queue():
    """ View for listing current jobs working """

    error = get_errors(request.args)

    def error_class(em):
        return {
            'failure': 'error',
            'pending': 'warning',
        	'success': 'success'
        	}.get(em, '') 

    p_list = list()
    p_list.append(Markup('<thead><tr><th>is_alive</th><th>PID</th><th>url</th><th>status</th></tr></thead>\n<tbody>\n'))
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
                set_data(p.request, q_response, pkl_data)

                p.status[0] = 'success'
                logging.info(__name__ + '::Completed request %s.' % p.url)

        except Exception as e:
            p.status[0] = 'failure'
            logging.error(__name__ + "::Could not update request: %s.  Exception: %s" % (p.url, e.message) )

        # Log the status of the job
        response_url = "".join(['<a href="', request.url_root, p.url + '">', p.url, '</a>'])
	
        p_list.append(Markup('<tr class="'+ error_class(p.status[0])+'"><td>'))
        p_list.append("</td><td>".join([str(p.process.is_alive()), str(p.process.pid),
                                  escape(Markup(response_url)), p.status[0]]))
        p_list.append(Markup('</td></tr>'))

    p_list.append(Markup('\n</tbody>'))

    if error:
        return render_template('queue.html', procs=p_list, error=error)
    else:
        return render_template('queue.html', procs=p_list)

@app.route('/all_requests')
def all_urls():
    """ View for listing all requests """

    # Build a tree containing nested key values
    tree = build_key_tree(pkl_data)
    key_sigs = list()

    # Depth first traversal - get the key signatures
    for node in tree:
        stack_trace = [node]
        while stack_trace:
            if stack_trace[-1]:
                ptr = stack_trace[-1][1]
                try: stack_trace.append(ptr.next())
                except StopIteration: stack_trace.pop() # no more children
            else:
                key_sigs.append([elem[0] for elem in stack_trace[:-1]])
                stack_trace.pop()

    # Compose urls from key sigs
    url_list = list()
    for key_sig in key_sigs:
        url = get_url_from_keys(key_sig)
        url_list.append("".join(['<a href="', request.url_root, url + '">', url, '</a>']))
    return render_template('all_urls.html', urls=url_list)


def build_key_tree(nested_dict):
    """ Builds a tree of key values from a nested dict """
    if hasattr(nested_dict, 'keys'):
        for key in nested_dict.keys(): yield (key, build_key_tree(nested_dict[key]))
    else:
        yield None

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
        app.run(debug=True)
    finally:
        del a



