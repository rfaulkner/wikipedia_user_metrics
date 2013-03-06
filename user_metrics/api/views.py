"""
    This module defines the flask application and views utilizing flask
    functionality to define leverage Jinja2_ templating system.

    .. _Jinja2: http://jinja.pocoo.org/docs/

    View & Method Definitions
    ~~~~~~~~~~~~~~~~~~~~~~~~~
"""

__author__ = {
    "dario taraborelli": "dario@wikimedia.org",
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2012-12-21"
__license__ = "GPL (version 2 or later)"


from flask import Flask, render_template, Markup, redirect, url_for, \
    request, escape, jsonify, make_response
from re import search, sub
from collections import OrderedDict

from user_metrics.etl.data_loader import Connector
from user_metrics.metrics.metrics_manager import get_metric_names
from user_metrics.config import logging
from user_metrics.utils import unpack_fields
from user_metrics.api.engine.data import build_key_tree, get_cohort_id, \
    get_cohort_refresh_datetime, get_data, get_url_from_keys, set_data, \
    build_key_signature, get_keys_from_tree
from user_metrics.api.engine import MW_UNAME_REGEX
from user_metrics.api import MetricsAPIError
from user_metrics.api.engine.request_meta import request_queue, \
    filter_request_input, format_request_params, RequestMetaFactory, \
    response_queue, rebuild_unpacked_request


# REGEX to identify refresh flags in the URL
REFRESH_REGEX = r'refresh[^&]*&|\?refresh[^&]*$|&refresh[^&]*$'

# Queue for storing all active processes
global requests_made
requests_made = OrderedDict()

# Stores cached requests (this should eventually be replaced with
# a proper cache)
api_data = OrderedDict()


# Error codes for web requests
global error_codes
error_codes = {
    0: 'Job already running.',
    1: 'Badly Formatted timestamp',
    2: 'Could not locate stored request.',
    3: 'Could not find User ID.',
    }


def get_errors(request_args):
    """ Returns the error string given the code in request_args """
    error = ''
    if 'error' in request_args:
        try:
            error = error_codes[int(request_args['error'])]
        except (KeyError, ValueError):
            pass
    return error


app = Flask(__name__)


@app.route('/')
def api_root():
    """ View for root url - API instructions """
    #@@@ TODO make tag list generation a dedicated method
    conn = Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn
    return render_template('index.html', cohort_data=data,
        m_list=get_metric_names())


@app.route('/about/')
def about():
    return render_template('about.html')


@app.route('/contact/')
def contact():
    return render_template('contact.html')


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
    conn = Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn
    #@@@ TODO validate user input against list of existing metrics
    return render_template('metric.html', m_str=metric, cohort_data=data)


@app.route('/user/<string:user>/<string:metric>')
def user_request(user, metric):
    """ View for requesting metrics for a single user """
    url = request.url.split(request.url_root)[1]

    # If it is a user name convert to ID
    if search(MW_UNAME_REGEX, user):
        # Extract project from query string
        # @TODO `project` should match what's in REQUEST_META_QUERY_STR
        project = request.args['project'] if 'project' in request.args\
        else 'enwiki'
        logging.debug(__name__ + '::Getting user id from name.')
        conn = Connector(instance='slave')
        conn._cur_.execute('SELECT user_id FROM {0}.user WHERE '
                           'user_name = "{1}"'.format(project,
            str(escape(user))))
        try:
            user_id = str(conn._cur_.fetchone()[0])
            url = sub(user, user_id, url)
        except Exception:
            logging.error(error_codes[3])
            return redirect(url_for('all_cohorts') + '?error=3')

    url = sub('user', 'cohorts', url)
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
        conn = Connector(instance='slave')
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
            m_list=get_metric_names(), error=error)


@app.route('/cohorts/<string:cohort>/<string:metric>')
def output(cohort, metric):
    """ View corresponding to a data request -
        All of the setup and execution for a request happens here. """

    process_responses()

    # Get URL.  Check for refresh flag - drop from url
    url = request.url.split(request.url_root)[1]
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

    # Build a request.
    # 1. Populate with request parameters from query args.
    # 2. Filter the input discarding any url junk
    # 3. Process defaults for request parameters
    rm = RequestMetaFactory(cohort, cohort_refresh_ts, metric)
    filter_request_input(request, rm)
    try:
        format_request_params(rm)
    except MetricsAPIError as e:
        return redirect(url_for('all_cohorts') + '?error=' + e.message)

    # Determine if the request maps to an existing response.
    # 1. The response already exists in the hash, return.
    # 2. Otherwise, add the request tot the queue.
    data = get_data(rm, api_data)
    if data and not refresh:
        return data
    else:
        # Add the request to the queue
        request_queue.put(unpack_fields(rm))
        key_sig = build_key_signature(rm, hash_result=True)
        requests_made[key_sig] = [True, url, rm]

    return render_template('processing.html', url_str=str(rm))


@app.route('/job_queue/')
def job_queue():
    """ View for listing current jobs working """

    process_responses()
    error = get_errors(request.args)

    p_list = list()
    p_list.append(Markup('<thead><tr><th>is_alive</th><th>url'
                         '</th></tr></thead>\n<tbody>\n'))
    for key in requests_made:
        # Log the status of the job

        url = requests_made[key][1]
        is_alive = str(requests_made[key][0])

        response_url = "".join(['<a href="',
                                request.url_root,
                                url + '">', url, '</a>'])
        p_list.append("</td><td>".join([is_alive,
                                        escape(Markup(response_url)),
                                        ]))
        p_list.append(Markup('</td></tr>'))
    p_list.append(Markup('\n</tbody>'))

    if error:
        return render_template('queue.html', procs=p_list, error=error)
    else:
        return render_template('queue.html', procs=p_list)


@app.route('/all_requests')
def all_urls():
    """ View for listing all requests.  Retireves from cache """

    # Build a tree containing nested key values
    tree = build_key_tree(api_data)
    key_sigs = get_keys_from_tree(tree)

    # Compose urls from key sigs
    url_list = list()
    for key_sig in key_sigs:

        url = get_url_from_keys(key_sig, 'stored')
        url_list.append("".join(['<a href="',
                                 request.url_root, url + '">',
                                 url,
                                 '</a>']))
    return render_template('all_urls.html', urls=url_list)


def process_responses():
    """ Pulls responses off of the queue. """
    while not response_queue.empty():
        res = response_queue.get()

        request_meta = rebuild_unpacked_request(res[0])
        key_sig = build_key_signature(request_meta, hash_result=True)

        # Set request in list to "not alive"
        if key_sig in requests_made:
            requests_made[key_sig][0] = False

        data = make_response(jsonify(res[1]))
        set_data(request_meta, data, api_data)
