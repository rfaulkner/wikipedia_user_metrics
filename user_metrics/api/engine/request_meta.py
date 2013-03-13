"""
    This module defines a container for API requests.

    Communication between URL requests and UserMetric
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    RequestMeta is a recordtype_ type (a mutable namedtuple) that is
    dynamically built at runtime to store API request parameters.  The
    list REQUEST_META_QUERY_STR contains all the possible query string
    variables that may be accepted by a request while REQUEST_META_BASE
    defines the URL path meta data (cohort and metric handles).  The
    factory method RequestMetaFactory is invoked by run.py to build
    a RequestMeta object.  For example::

        rm = RequestMetaFactory("cohort name", "cohort timestamp", "metric")

    Finally, a mediator_ pattern via the varMapping namedtuple type is
    used to bind the names of URL request variables to corresponding UserMetric
    parameter names.  The definition of the mapping lives in this module.
    The factory method returns the newly built RequestMeta which may then
    be populated with parameter values.  The ``process_request_params`` method
    applies defaults to RequestMeta objects.  The run module handles assigning
    request values to RequestMeta attributes and coordinating the passage of
    this data to UserMetric objects via metrics_manager_ using the
    ``process_metrics`` method.

    .. _recordtype: http://www.python.org/
    .. _mediator: http://en.wikipedia.org/wiki/Mediator_pattern
    .. _metrics_manager: http://www.python.org/

"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "2013-03-05"
__license__ = "GPL (version 2 or later)"


from user_metrics.utils.record_type import recordtype
from dateutil.parser import parse as date_parse
import user_metrics.metrics.metrics_manager as mm
from user_metrics.api import MetricsAPIError
from user_metrics.api.engine import DEFAULT_QUERY_VAL, DATETIME_STR_FORMAT
from collections import namedtuple
from flask import escape
from multiprocessing import Queue
from user_metrics.config import logging


# Structure that maps values in the query string to new ones
REQUEST_VALUE_MAPPING = {
    'group': {
        'reg': 0,
        'input': 1,
        'reginput': 2,
    }
}

# Define standard variable names in the query string - store in named tuple
RequestMeta = recordtype('RequestMeta',
                         'cohort_expr cohort_gen_timestamp metric '
                         'time_series aggregator project '
                         'namespace start end interval t n  group')


# API queues for API service requests and responses
request_queue = Queue()
response_queue = Queue()


def RequestMetaFactory(cohort_expr, cohort_gen_timestamp, metric_expr):
    """
        Dynamically builds a record type given a metric handle

        All args must be strings representing a cohort, last updated
        timestamp, and metric respectively.

            **cohort_expr**             - string. Cohort id from url.
            **cohort_gen_timestamp**    - string. Timestamp of last cohort
            update.
            **metric_expr**             - string. Metric id from url.
    """
    default_params = 'cohort_expr cohort_gen_timestamp metric '
    additional_params = ''
    for val in QUERY_PARAMS_BY_METRIC[metric_expr]:
        additional_params += val.query_var + ' '
    additional_params = additional_params[:-1]
    params = default_params + additional_params

    arg_list = ['cohort_expr', 'cohort_gen_timestamp', 'metric_expr'] +\
               ['None'] * len(QUERY_PARAMS_BY_METRIC[metric_expr])
    arg_str = "(" + ",".join(arg_list) + ")"

    rt = recordtype("RequestMeta", params)
    return eval('rt' + arg_str)


REQUEST_META_QUERY_STR = ['aggregator', 'time_series', 'project', 'namespace',
                          'start', 'end', 'interval', 't', 'n',
                          'time_unit', 'time_unit_count', 'look_ahead',
                          'look_back', 'threshold_type', 'group']
REQUEST_META_BASE = ['cohort_expr', 'metric']


# Using the MEDIATOR model :: Defines the query parameters accepted by each
# metric request.  This is a dict keyed on
# metric that stores a list of tuples.  Each tuple defines:
#
#       (<name of allowable query string var>, <name of corresponding
#       metric param>)

# defines a tuple for mapped variable names
varMapping = namedtuple("VarMapping", "query_var metric_var")

common_params = [varMapping('start', 'datetime_start'),
                 varMapping('end', 'datetime_end'),
                 varMapping('project', 'project'),
                 varMapping('namespace', 'namespace'),
                 varMapping('interval', 'interval'),
                 varMapping('time_series', 'time_series'),
                 varMapping('aggregator', 'aggregator'),
                 varMapping('t', 't'),
                 varMapping('group', 'group')]

QUERY_PARAMS_BY_METRIC = {
    'blocks': common_params,
    'bytes_added': common_params,
    'edit_count': common_params,
    'edit_rate': common_params + [varMapping('time_unit', 'time_unit'),
                                  varMapping('time_unit_count',
                                             'time_unit_count')],
    'live_account': common_params,
    'namespace_edits': common_params,
    'revert_rate': common_params + [varMapping('look_back', 'look_back'),
                                    varMapping('look_ahead', 'look_ahead')],
    'survival': common_params,
    'threshold': common_params + [varMapping('n', 'n')],
    'time_to_threshold': common_params + [varMapping('threshold_type',
                                                     'threshold_type_class')],
}


def format_request_params(request_meta):
    """
        Formats request data and ensures that it is clean using Flask escape
        functionality.

            Parameters
            ~~~~~~~~~~

            request_meta : recordtype:
                Stores the request data.
    """

    TIME_STR = '000000'

    # Handle any datetime fields passed - raise an exception if the
    # formatting is incorrect
    if request_meta.start:
        try:
            request_meta.start = date_parse(
                escape(request_meta.start)).strftime(
                    DATETIME_STR_FORMAT)[:8] + TIME_STR
        except ValueError:
            # Pass the value of the error code in `error_codes`
            raise MetricsAPIError('1')

    if request_meta.end:
        try:
            request_meta.end = date_parse(
                escape(request_meta.end)).strftime(
                    DATETIME_STR_FORMAT)[:8] + TIME_STR
        except ValueError:
            # Pass the value of the error code in `error_codes`
            raise MetricsAPIError('1')

    # set the aggregator if there is one
    agg_key = mm.get_agg_key(request_meta.aggregator, request_meta.metric)
    request_meta.aggregator = escape(request_meta.aggregator)\
        if agg_key else None
    # @TODO Escape remaining input

    # MAP request values.
    _map_request_values(request_meta)


def _map_request_values(request_meta):
    """
        Map values from the request.  Use ``REQUEST_VALUE_MAPPING`` convert
        coded values from the request if a familiar encoding is present.

            Parameters
            ~~~~~~~~~~

            request_meta : recordtype:
                Stores the request data.
    """
    for attr in REQUEST_VALUE_MAPPING:
        if hasattr(request_meta, attr):
            request_value = None
            try:
                request_value = getattr(request_meta, attr)
                map_val = REQUEST_VALUE_MAPPING[attr][request_value]
                setattr(request_meta, attr, map_val)
            except KeyError:
                logging.error(__name__ + ' :: Could not map request value '
                                         '{0} for variable {1}.'.
                              format(str(request_value), attr))


def filter_request_input(request, request_meta_obj):
    """
        Filters for relevant request data and sets RequestMeta object.

        Parameters
        ~~~~~~~~~~

            **request_meta_obj** - RequestMeta object to store relevant request
            data
            **request** - Flask request object containing all request data
    """
    if not hasattr(request, 'args'):
        raise MetricsAPIError('Flask request must have "args" attribute.')

    for param in REQUEST_META_QUERY_STR:
        if param in request.args and hasattr(request_meta_obj, param):
            if not request.args[param]:
                # Assign a value indicating presence of a query var
                setattr(request_meta_obj, param, DEFAULT_QUERY_VAL)
            else:
                setattr(request_meta_obj, param, request.args[param])


def rebuild_unpacked_request(unpacked_req):
    """
        Takes an unpacked (user_metrics.utils.unpack_fields) RequestMeta object
        and composes a RequestMeta object

        Parameters
        ~~~~~~~~~~

            unpacked_req : dict
                This dictionary contains keys that map to the attributes of the
                ``RequestMeta`` type.
    """
    try:
        # Build the request item
        rm = RequestMetaFactory(unpacked_req['cohort_expr'],
                                unpacked_req['cohort_gen_timestamp'],
                                unpacked_req['metric'])

        # Populate the request data
        for key in unpacked_req:
            if unpacked_req[key]:
                setattr(rm, key, unpacked_req[key])
        return rm
    except KeyError:
        raise MetricsAPIError(__name__ + ' :: rebuild_unpacked_request - '
                                         'Invalid fields.')
