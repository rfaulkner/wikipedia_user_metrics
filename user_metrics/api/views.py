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
    request, escape, flash, jsonify, make_response
from re import search, sub
from multiprocessing import Lock

from user_metrics.etl.data_loader import Connector
from user_metrics.config import logging, settings
from user_metrics.utils import unpack_fields
from user_metrics.api.engine.data import get_cohort_id, \
    get_cohort_refresh_datetime, get_data, get_url_from_keys, \
    build_key_signature, read_pickle_data
from user_metrics.api.engine import MW_UNAME_REGEX
from user_metrics.api import MetricsAPIError, error_codes
from user_metrics.api.engine.request_meta import filter_request_input, \
    format_request_params, RequestMetaFactory, \
    get_metric_names
from user_metrics.api.engine.request_manager import api_request_queue, \
    req_cb_get_cache_keys, req_cb_get_url, req_cb_get_is_running, \
    req_cb_add_req

from user_metrics.api.session import APIUser

# View Lock for atomic operations
VIEW_LOCK = Lock()

# Instantiate flask app
app = Flask(__name__)

# REGEX to identify refresh flags in the URL
REFRESH_REGEX = r'refresh[^&]*&|\?refresh[^&]*$|&refresh[^&]*$'


def get_errors(request_args):
    """ Returns the error string given the code in request_args """
    error = ''
    if 'error' in request_args:
        try:
            error = error_codes[int(request_args['error'])]
        except (KeyError, ValueError):
            pass
    return error


# Views
# #####

# Flask Login views

if settings.__flask_login_exists__:

    from flask.ext.login import login_required, logout_user, \
        confirm_login, login_user, fresh_login_required, current_user

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST' and 'username' in request.form:

            username = escape(unicode(str(request.form['username'])))
            passwd = escape(unicode(str(request.form['password'])))
            remember = request.form.get('remember', 'no') == 'yes'

            # Initialize user
            user_ref = APIUser(username)
            user_ref.authenticate(passwd)

            logging.debug(__name__ + ' :: Authenticating "{0}"/"{1}" ...'.
                format(username, passwd))

            if user_ref.is_authenticated():
                login_user(user_ref, remember=remember)
                flash('Logged in.')
                return redirect(request.args.get('next')
                                or url_for('api_root'))
            else:
                flash('Login failed.')
        return render_template('login.html')

    @app.route('/reauth', methods=['GET', 'POST'])
    @login_required
    def reauth():
        if request.method == 'POST':
            confirm_login()
            flash(u'Reauthenticated.')
            return redirect(request.args.get('next') or url_for('api_root'))
        return render_template('reauth.html')

    @app.route('/logout')
    def logout():
        logout_user()
        flash('Logged out.')
        return redirect(url_for('api_root'))

else:

    def login_required(f):
        """ Does Nothing."""
        def wrap(*args, **kwargs):
            f(*args, **kwargs)
        return wrap()


# API views

def api_root():
    """ View for root url - API instructions """
    #@@@ TODO make tag list generation a dedicated method
    conn = Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn

    if settings.__flask_login_exists__ and current_user.is_anonymous():
        return render_template('index_anon.html', cohort_data=data,
                               m_list=get_metric_names())
    else:
        return render_template('index.html', cohort_data=data,
                               m_list=get_metric_names())


def about():
    return render_template('about.html')


def contact():
    return render_template('contact.html')


def all_metrics():
    """ Display a list of available metrics """
    if request.method == 'POST':
        #@@@ TODO  validate form input against existing cohorts
        return metric(request.form['selectMetric'])
    else:
        return render_template('all_metrics.html')


def metric(metric=''):
    """ Display single metric documentation """
    #@@@ TODO make tag list generation a dedicated method
    conn = Connector(instance='slave')
    conn._cur_.execute('select utm_name from usertags_meta')
    data = [r[0] for r in conn._cur_]
    del conn
    #@@@ TODO validate user input against list of existing metrics
    return render_template('metric.html', m_str=metric, cohort_data=data)


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

    # redirect to output view
    url = sub('user', 'cohorts', url)
    return redirect(url)


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


def cohort(cohort=''):
    """ View single cohort page """
    error = get_errors(request.args)
    if not cohort:
        return redirect(url_for('all_cohorts'))
    else:
        return render_template('cohort.html', c_str=cohort,
                               m_list=get_metric_names(), error=error)


def output(cohort, metric):
    """ View corresponding to a data request -
        All of the setup and execution for a request happens here. """

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
        logging.error(__name__ + ' :: Could not retrieve refresh '
                                 'time of cohort.')

    # Build a request.
    # 1. Populate with request parameters from query args.
    # 2. Filter the input discarding any url junk
    # 3. Process defaults for request parameters
    try:
        rm = RequestMetaFactory(cohort, cohort_refresh_ts, metric)
    except MetricsAPIError as e:
        return redirect(url_for('all_cohorts') + '?error=' +
                        str(e.error_code))

    filter_request_input(request, rm)
    try:
        format_request_params(rm)
    except MetricsAPIError as e:
        return redirect(url_for('all_cohorts') + '?error=' +
                        str(e.error_code))

    # Determine if the request maps to an existing response.
    # 1. The response already exists in the hash, return.
    # 2. Otherwise, add the request tot the queue.
    data = get_data(rm)
    key_sig = build_key_signature(rm, hash_result=True)

    # Is the request already running?
    is_running = req_cb_get_is_running(key_sig, VIEW_LOCK)

    # Determine if request is already hashed
    if data and not refresh:
        return make_response(jsonify(data))

    # Determine if the job is already running
    elif is_running:
        return render_template('processing.html',
                               error=error_codes[0],
                               url_str=str(rm))

    # Add the request to the queue
    else:
        api_request_queue.put(unpack_fields(rm), block=True)
        req_cb_add_req(key_sig, url, VIEW_LOCK)

    return render_template('processing.html', url_str=str(rm))


def job_queue():
    """ View for listing current jobs working """

    error = get_errors(request.args)

    p_list = list()
    p_list.append(Markup('<thead><tr><th>is_alive</th><th>url'
                         '</th></tr></thead>\n<tbody>\n'))

    keys = req_cb_get_cache_keys(VIEW_LOCK)
    for key in keys:
        # Log the status of the job
        url = req_cb_get_url(key, VIEW_LOCK)
        is_alive = str(req_cb_get_is_running(key, VIEW_LOCK))

        p_list.append('<tr><td>')
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


def all_urls():
    """ View for listing all requests.  Retrieves from cache """

    # @TODO - this reads the entire cache into memory, filters will be needed
    # This extracts ALL data from the cache, the data is assumed to be in the
    # form of <hash key -> (data, key signature)> pairs.  The key signature is
    # extracted to reconstruct the url.

    all_data = read_pickle_data()
    key_sigs = list()

    for key, val in all_data.iteritems():
        if hasattr(val, '__iter__'):
            try:
                key_sigs.append(val[1])
            except (KeyError, IndexError):
                logging.error(__name__ + ' :: Could not render key signature '
                                         'from data, key = {0}'.format(key))

    # Compose urls from key sigs
    url_list = list()
    for key_sig in key_sigs:

        url = get_url_from_keys(key_sig, 'cohorts')
        url_list.append("".join(['<a href="',
                                 request.url_root, url + '">',
                                 url,
                                 '</a>']))
    return render_template('all_urls.html', urls=url_list)


def thin_client_view():
    """
        View for handling requests outside sessions.  Useful for processing
        jobs from https://github.com/rfaulkner/umapi_client.

        Returns:

            1) JSON response if the request is complete
            2) Validation response (minimal size)
    """

    # Validate key
    # Construct request meta
    # Check for job cached
    #   If YES return
    #   If NO queue job, return verify

    return None


# Add View Decorators
# ##

# Stores view references in structure
view_list = {
    api_root.__name__: api_root,
    all_urls.__name__: all_urls,
    job_queue.__name__: job_queue,
    output.__name__: output,
    cohort.__name__: cohort,
    all_cohorts.__name__: all_cohorts,
    user_request.__name__: user_request,
    metric.__name__: metric,
    all_metrics.__name__: all_metrics,
    about.__name__: about,
    contact.__name__: contact,
    thin_client_view.__name__: thin_client_view
}

# Dict stores routing paths for each view
route_deco = {
    api_root.__name__: app.route('/'),
    all_urls.__name__: app.route('/all_requests'),
    job_queue.__name__: app.route('/job_queue/'),
    output.__name__: app.route('/cohorts/<string:cohort>/<string:metric>'),
    cohort.__name__: app.route('/cohorts/<string:cohort>'),
    all_cohorts.__name__: app.route('/cohorts/', methods=['POST', 'GET']),
    user_request.__name__: app.route('/user/<string:user>/<string:metric>'),
    metric.__name__: app.route('/metrics/<string:metric>'),
    all_metrics.__name__: app.route('/metrics/', methods=['POST', 'GET']),
    about.__name__: app.route('/about/'),
    contact.__name__: app.route('/contact/'),
    thin_client_view.__name__: app.route('/thin/<string:cohort>/<string:metric>')
}

# Dict stores flag for login required on view
login_req_deco = {
    api_root.__name__: False,
    all_urls.__name__: True,
    job_queue.__name__: True,
    output.__name__: True,
    cohort.__name__: True,
    all_cohorts.__name__: True,
    user_request.__name__: True,
    metric.__name__: True,
    all_metrics.__name__: False,
    about.__name__: False,
    contact.__name__: False,
    thin_client_view.__name__: False
}

# Apply decorators to views

if settings.__flask_login_exists__:
    for key in login_req_deco:
        view_method = view_list[key]
        if login_req_deco[key]:
            view_list[key] = login_required(view_method)

for key in route_deco:
    route = route_deco[key]
    view_method = view_list[key]
    view_list[key] = route(view_method)