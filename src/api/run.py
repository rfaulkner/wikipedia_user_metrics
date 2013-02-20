#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    This module defines the entry point for flask_ web server implementation
    Wikimedia User Metrics API.  This module utilizes flask functionality
    to define leverage Jinja2_ templating system.  This module is consumable
    by the Apache web server via WSGI interface via mod_wsgi.  An Apache
    server can be pointed to api.wsgi such that Apache may be used as a
    wrapper in this way.

    .. _flask: http://flask.pocoo.org
    .. _Jinja2: http://jinja.pocoo.org/docs/

    Job Queue and Processing
    ^^^^^^^^^^^^^^^^^^^^^^^^

    As requests are issued via http to the API a process queue will store all
    active jobs. Processes will be created and assume one of the following
    states throughout their existence: ::

        * 'pending' - The request has yet to be fully processed and exposed
            but is underway
        * 'success' - The request has finished processing and is exposed at
            the url
        * 'failure' - The result has finished processing but dailed to expose
            results

    When a process a request is received and a job is created to service that
    request it enters the 'pending' state. If the job returns without
    exception it enters the 'success' state, otherwise it enters the 'failure'
    state.  The job remains in either of these states until it is cleared
    from the process queue.

    Response Data
    ^^^^^^^^^^^^^

    As requests are made to the API the data generated and formatted as JSON.
    The definition of is as follows: ::

        {   header : header_list,
            cohort_expr : cohort_gen_timestamp : metric : timeseries :
            aggregator : date_start : date_end : [ metric_param : ]* : data
        }

    Where each component is defined: ::

        header_str := list(str), list of header values
        cohort_expr := str, cohort ID expression
        cohort_gen_timestamp := str, cohort generation timestamp (earliest of
            all cohorts in expression)
        metric := str, user metric handle
        timeseries := boolean, indicates if this is a timeseries
        aggregator := str, aggregator used
        date_start := str, start datetime of request
        date_end := str, end datetime of request
        metric_param := -, optional metric parameters
        data := list(tuple), set of data points

    Request data is mapped to a query via metric objects and hashed in the
    dictionary `pkl_data`.

    Cohort Data
    ^^^^^^^^^^^

    Cohort data is maintained in the host s1-analytics-slave.eqiad.wmnet under
    the `staging` database in the `usertags` and `usertags_meta` tables: ::

        +---------+-----------------+------+-----+---------+-------+
        | Field   | Type            | Null | Key | Default | Extra |
        +---------+-----------------+------+-----+---------+-------+
        | ut_user | int(5) unsigned | NO   | PRI | NULL    |       |
        | ut_tag  | int(4) unsigned | NO   | PRI | NULL    |       |
        +---------+-----------------+------+-----+---------+-------+

        +-------------+-----------------+------+-----+---------+
        | Field       | Type            | Null | Key | Default |
        +-------------+-----------------+------+-----+---------+
        | utm_id      | int(5) unsigned | NO   | PRI | NULL    |
        | utm_name    | varchar(255)    | NO   |     |         |
        | utm_notes   | varchar(255)    | YES  |     | NULL    |
        | utm_touched | datetime        | YES  |     | NULL    |
        +-------------+-----------------+------+-----+---------+

    View & Method Definitions
    ~~~~~~~~~~~~~~~~~~~~~~~~~
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
from re import sub, search
from shutil import copyfile

from src.metrics.users import MediaWikiUser
import src.etl.data_loader as dl
import src.metrics.metrics_manager as mm

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

# Stores cached requests (this should eventually be replaced
# with a proper cache)
global pkl_data
pkl_data = OrderedDict()

# stores data in Queue objects that are active
global queue_data
queue_data = dict()

# Error codes for web requests
global error_codes
error_codes = {
    0: 'Job already running.',
    1: 'Badly Formatted timestamp',
    2: 'Could not locate stored request.',
    3: 'Could not find User ID.',
}

# Queue for storing all active processes
global processQ
processQ = list()

# Class defining all objects contained on the processQ
QStructClass = collections.namedtuple('QStruct', 'id process request url '
                                                 'queue status')


# REGEX to identify refresh flags in the URL
REFRESH_REGEX = r'refresh[^&]*&|\?refresh[^&]*$|&refresh[^&]*$'

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
        except KeyError:
            pass
        except ValueError:
            pass
    return error


def process_metrics(p, rm):
    """ Worker process for requests -
        this will typically operate in a forked process """

    conn = dl.Connector(instance='slave')
    logging.info(__name__ + '::START JOB %s (PID = %s)' % (str(rm),
                                                           os.getpid()))

    # obtain user list - handle the case where a lone user ID is passed
    if search(MW_UID_REGEX, str(rm.cohort_expr)):
        users = [rm.cohort_expr]
    # Special case where user lists are to be generated based on registered
    # user reg dates from the logging table -- see src/metrics/users.py
    elif rm.cohort_expr == 'all':
        users = MediaWikiUser(query_type=1)
    else:
        users = get_users(rm.cohort_expr)

    # unpack RequestMeta into dict using MEDIATOR
    args = {attr.metric_var: getattr(rm, attr.query_var)
            for attr in QUERY_PARAMS_BY_METRIC[rm.metric]}
    logging.info(__name__ + '::Calling %s with args = %s.' % (rm.metric,
                                                              str(args)))

    # process request
    results = mm.process_data_request(rm.metric, users, **args)

    p.put(jsonify(results))
    del conn
    logging.info(__name__ + '::END JOB %s (PID = %s)' % (str(rm), os.getpid()))


######
#
# Define View methods
#
#######


@app.route('/')
def api_root():
    """ View for root url - API instructions """
    #@@@ TODO make tag list generation a dedicated method
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn
    return render_template('index.html', cohort_data=data,
                           m_list=mm.get_metric_names())


@app.route('/about/')
def about():
    return render_template('about.html')


@app.route('/contact/')
def contact():
    return render_template('contact.html')

@app.route('/compare/')
def compare():
    return render_template('compare.html')

@app.route('/tags/') #deprecated
def tags():
    """ View for tag definitions where cohort meta data can be reviewed """
    #@@@ TODO make tag list generation a dedicated method
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
    #@@@ TODO make tag list generation a dedicated method
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn
    #@@@ TODO validate user input against list of existing metrics
    return render_template('metric.html', m_str=metric, cohort_data=data)

@app.route('/users/', methods=['POST', 'GET'])
def all_users():
    """ Display landing page for pulling single-user metrics """
    if request.method == 'POST':
        #@@@ TODO  validate form input
        return single_user(request.form['selectUser'])
    else:
        return render_template('all_users.html')

@app.route('/users/<string:user>', methods=['POST', 'GET'])
def single_user(user=''):
    """ Single user landing page """
    if not user:
        return redirect(url_for('all_users'))
    else:
        return render_template('user.html', user=user, m_list=mm.get_metric_names())

@app.route('/users/<string:user>/<string:metric>')
def user_request(user, metric):
    """ View for requesting metrics for a single user """
    url = request.url.split(request.url_root)[1]

    # If it is a user name convert to ID
    if search(MW_UNAME_REGEX, user):
        # Extract project from query string
        # @TODO `project` should match what's in REQUEST_META_QUERY_STR
        project = request.args['project'] if 'project' in request.args \
            else 'enwiki'
        logging.debug(__name__ + '::Getting user id from name.')
        conn = dl.Connector(instance='slave')
        conn._cur_.execute('SELECT user_id FROM {0}.user WHERE '
                           'user_name = "{1}"'.format(project,
                                                      str(escape(user))))
        try:
            user_id = str(conn._cur_.fetchone()[0])
            url = sub(user, user_id, url)
        except Exception:
            logging.error(error_codes[3])
            return redirect(url_for('all_cohorts') + '?error=3')

    url = sub('users','cohorts', url)
    return redirect(url)


@app.route('/cohorts/', methods=['POST', 'GET'])
def all_cohorts():
    """ View for listing and selecting cohorts """
    error = get_errors(request.args)

    #@@@ TODO  form validation against existing cohorts and metrics
    if request.method == 'POST':
        #@@@ TODO  validate form input against existing cohorts
        return cohort(request.form['selectCohort'])
    else:
        #@@@ TODO make tag list generation a dedicated method
        conn = dl.Connector(instance='slave')
        conn._cur_.execute('select distinct utm_name from usertags_meta')
        o = [r[0] for r in conn._cur_]
        del conn
        return render_template('all_cohorts.html', data=o, error=error)


@app.route('/cohorts/<string:cohort>')
def cohort(cohort=''):
    """ View single cohort page """
    error = get_errors(request.args)
    if not cohort:
        return redirect(url_for('all_cohorts'))
    else:
        return render_template('cohort.html', c_str=cohort,
                               m_list=mm.get_metric_names(), error=error)


@app.route('/cohorts/<string:cohort>/<string:metric>')
def cohort_job_review(cohort, metric):
    """ Review job before firing it """
    return render_template('cohort_review.html', c=cohort, m=metric)

@app.route('/run/<string:cohort>/<string:metric>', methods=['GET'])
def output(cohort, metric):
    """ View corresponding to a data request -
        All of the setup and execution for a request happens here. """

    global global_id
    url = request.url.split(request.url_root)[1]

    # Check for refresh flag - drop from url
    refresh = True if 'refresh' in request.args else False
    if refresh:
        url = sub(REFRESH_REGEX, '', url)

    # Get the refresh date of the cohort
    try:
        cid = get_cohort_id(cohort)
        cohort_refresh_ts = get_cohort_refresh_datetime(cid)
    except Exception:
        cohort_refresh_ts = None
        logging.error(__name__ + '::Could not retrieve refresh '
                                 'time of cohort.')

    # Build a request. Populate with request parameters from query args.
    # Filter the input discarding any url junk
    rm = RequestMetaFactory(cohort, cohort_refresh_ts, metric)
    filter_request_input(request, rm)

    # Process defaults for request parameters
    try:
        process_request_params(rm)
    except MetricsAPIError as e:
        return redirect(url_for('all_cohorts') + '?error=' + e.message)

    # Determine if the request maps to an existing response.  If so return it.
    # Otherwise compute.
    data = get_data(rm, pkl_data)
    if data and not refresh:
        return data
    else:

        # Ensure that the job for this url is not already running
        is_pending_job = False
        for p in processQ:
            if not cmp(rm, p.request) and p.status[0] == 'pending':
                is_pending_job = True

        # Queue the job
        if not is_pending_job:

            q = mp.Queue()
            p = mp.Process(target=process_metrics, args=(q, rm))
            p.start()

            global_id += 1

            logging.info(__name__ + '::Appending request %s to the queue...'
                                    % rm)
            processQ.append(QStructClass(global_id, p, rm, url, q,
                                         ['pending']))
            return render_template('processing.html', url_str=str(rm))
        else:
            return redirect(url_for('job_queue') + '?error=0')

@app.route('/job_queue/') #backward compatibility
@app.route('/jobs/')
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
    p_list.append(Markup('<thead><tr><th>is_alive</th><th>PID</th><th>url'
                         '</th><th>status</th></tr></thead>\n<tbody>\n'))
    for p in processQ:
        try:

            # Pull data off of the queue and add it to the queue data
            while not p.queue.empty():
                if not p.id in queue_data:
                    queue_data[p.id] = json.loads(p.queue.get().data)
                else:
                    for k, v in queue_data[p.id]:
                        if hasattr(v, '__iter__'):
                            queue_data[p.id][k].extend(v)

            # once a process has finished working remove it and put its
            # contents into the cache
            if not p.process.is_alive() and p.status[0] == 'pending':
                q_response = make_response(jsonify(queue_data[p.id]))
                del queue_data[p.id]
                set_data(p.request, q_response, pkl_data)

                p.status[0] = 'success'
                logging.info(__name__ + '::Completed request %s.' % p.url)

        except Exception as e:
            p.status[0] = 'failure'
            logging.error(__name__ + "::Could not update request: %s.  "
                                     "Exception: %s" % (p.url, e.message))

        # Log the status of the job
        response_url = "".join(['<a href="',
                               request.url_root, p.url + '">', p.url, '</a>'])
        p_list.append(Markup('<tr class="' + error_class(p.status[0]) +
                             '"><td>'))
        p_list.append("</td><td>".join([str(p.process.is_alive()),
                                        str(p.process.pid),
                                        escape(Markup(response_url)),
                                        p.status[0]]))
        p_list.append(Markup('</td></tr>'))
    p_list.append(Markup('\n</tbody>'))

    if error:
        return render_template('queue.html', procs=p_list, error=error)
    else:
        return render_template('queue.html', procs=p_list)


@app.route('/all_requests')
def all_requests():
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
                try:
                    stack_trace.append(ptr.next())
                except StopIteration:
                    # no more children
                    stack_trace.pop()
            else:
                key_sigs.append([elem[0] for elem in stack_trace[:-1]])
                stack_trace.pop()

    # Compose urls from key sigs
    url_list = list()
    for key_sig in key_sigs:
        url = get_url_from_keys(key_sig, 'stored')
        url_list.append("".join(['<a href="',
                                 request.url_root, url + '">', url, '</a>']))
    return render_template('all_requests.html', urls=url_list)


@app.route('/stored/<string:cohort>/<string:metric>')
def stored_requests(cohort, metric):
    """ View for processing stored requests """
    global pkl_data
    hash_ref = pkl_data

    # Parse the cohort and metric IDs
    try:
        hash_ref = hash_ref['cohort_expr' + HASH_KEY_DELIMETER + cohort][
            'metric' + HASH_KEY_DELIMETER + metric]
    except Exception:
        logging.error(__name__ + '::Request not found for: %s' % request.url)
        return redirect(url_for('cohorts') + '?error=2')

    # Parse the parameter values
    for param in REQUEST_META_QUERY_STR:
        if param in request.args:
            try:
                hash_ref = hash_ref[param + HASH_KEY_DELIMETER +
                                    request.args[param]]
            except KeyError:
                logging.error(__name__ + '::Request not found for: %s' %
                                         request.url)
                return redirect(url_for('cohorts') + '?error=2')

    # Ensure that that the data is a HTTP response object
    if hasattr(hash_ref, 'status_code'):
        return hash_ref
    else:
        return redirect(url_for('cohort') + '?error=2')

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
        global bad_pickle

        # Open the pickled data for reading.
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'rb')
        except IOError:
            pkl_file = None

        # test whether the open was successful
        if pkl_file:
            try:
                pkl_data = cPickle.load(pkl_file)
            except ValueError:
                # Generally due to a "insecure string pickle"
                logging.error(__name__ + ':: Could not access pickle data.')
                pkl_data = OrderedDict()

                # Move the bad pickle data into a new file and recreate the
                # original as an empty file
                src = settings.__data_file_dir__ + 'api_data.pkl'
                dest = settings.__data_file_dir__ + 'api_data.pkl.bad'

                copyfile(src, dest)
                os.remove(src)
                with open(src, 'wb'):
                    pass

            pkl_file.close()

    def close(self):
        """  When the instance is deleted store the pickled data """
        global pkl_data

        pkl_file = None
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'wb')
            cPickle.dump(pkl_data, pkl_file)
        except Exception:
            logging.error(__name__ + '::Could not pickle data.')
        finally:
            if hasattr(pkl_file, 'close'):
                pkl_file.close()

######
#
# Execution
#
#######


if __name__ == '__main__':

    # initialize API data - get the instance
    a = APIMethods()
    try:
        app.run(debug=True)
    finally:
        a.close()
