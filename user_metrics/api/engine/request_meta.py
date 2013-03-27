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
    this data to UserMetric objects via request_manager_ using the
    ``process_metrics`` method.

    .. _recordtype: http://www.python.org/
    .. _mediator: http://en.wikipedia.org/wiki/Mediator_pattern
    .. _request_manager: http://www.python.org/

"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "2013-03-05"
__license__ = "GPL (version 2 or later)"


from user_metrics.utils import format_mediawiki_timestamp, enum
from user_metrics.utils.record_type import recordtype
from user_metrics.api import MetricsAPIError
from user_metrics.api.engine import DEFAULT_QUERY_VAL
from collections import namedtuple, OrderedDict
from flask import escape
from user_metrics.config import logging
from user_metrics.utils import unpack_fields


# DEFINE REQUEST META OBJECT, CREATION, AND PROCESSING
# ####################################################


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

    try:
        metric_params = ParameterMapping.QUERY_PARAMS_BY_METRIC[metric_expr]
    except KeyError:
        raise MetricsAPIError('Bad metric name.', error_code=4)

    for val in metric_params:
        additional_params += val.query_var + ' '
    additional_params = additional_params[:-1]
    params = default_params + additional_params

    arg_list = ['cohort_expr', 'cohort_gen_timestamp', 'metric_expr'] +\
               ['None'] * \
               len(ParameterMapping.QUERY_PARAMS_BY_METRIC[metric_expr])
    arg_str = "(" + ",".join(arg_list) + ")"

    rt = recordtype("RequestMeta", params)
    return eval('rt' + arg_str)

# Defines what variables may be extracted from the query string
REQUEST_META_QUERY_STR = ['aggregator', 'time_series', 'project', 'namespace',
                          'start', 'end', 'interval', 't', 'n',
                          'time_unit', 'time_unit_count', 'look_ahead',
                          'look_back', 'threshold_type', 'group']

# Defines which variables may be taken from the URL path
REQUEST_META_BASE = ['cohort_expr', 'metric']


def format_request_params(request_meta):
    """
        Formats request data and ensures that it is clean using Flask escape
        functionality.

            Parameters
            ~~~~~~~~~~

            request_meta : recordtype:
                Stores the request data.
    """

    # Handle any datetime fields passed - raise an exception if the
    # formatting is incorrect
    if request_meta.start:
        try:
            request_meta.start = format_mediawiki_timestamp(
                escape(request_meta.start))
        except ValueError:
            # Pass the value of the error code in `error_codes`
            raise MetricsAPIError(error_code=1)

    if request_meta.end:
        try:
            request_meta.end = format_mediawiki_timestamp(
                escape(request_meta.end))
        except ValueError:
            # Pass the value of the error code in `error_codes`
            raise MetricsAPIError(error_code=1)

    # set the aggregator if there is one
    agg_key = get_agg_key(request_meta.aggregator, request_meta.metric)
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


# DEFINE MAPPING AMONG API REQUESTS AND METRICS
# #############################################


class ParameterMapping(object):
    """
        Using the **Mediator** model :: Defines the query parameters accepted by
        each metric request.  This is a dict keyed on metric that stores a list
        of tuples.  Each tuple defines:

           (<name of allowable query string var>, <name of corresponding
           metric param>)

    """

    # Singleton instance
    __instance = None

    def __init__(self):
        """ Initialize the Singleton instance """
        self.__class__.__instance = self

    def __new__(cls):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(ParameterMapping, cls).__new__(cls)
        return cls.__instance

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
                                        varMapping('look_ahead',
                                                   'look_ahead')],
        'survival': common_params,
        'threshold': common_params + [varMapping('n', 'n')],
        'time_to_threshold': common_params +
        [varMapping('threshold_type', 'threshold_type_class')],
    }

    @staticmethod
    def map(request_meta):
        """
            Unpack RequestMeta into dict using MEDIATOR Map parameters from
            API request to metrics call.
        """
        args = unpack_fields(request_meta)
        new_args = OrderedDict()

        for mapping in ParameterMapping.\
                QUERY_PARAMS_BY_METRIC[request_meta.metric]:
            new_args[mapping.metric_var] = args[mapping.query_var]
        return new_args


# DEFINE METRIC AND AGGREGATOR ENUMS ALLOWABLE IN REQUESTS
# ########################################################

from user_metrics.metrics.threshold import Threshold, threshold_editors_agg
from user_metrics.metrics.blocks import Blocks, block_rate_agg
from user_metrics.metrics.bytes_added import BytesAdded, ba_median_agg, \
    ba_min_agg, ba_max_agg, ba_sum_agg, ba_mean_agg, ba_std_agg
from user_metrics.metrics.survival import Survival, survival_editors_agg
from user_metrics.metrics.revert_rate import RevertRate, revert_rate_avg
from user_metrics.metrics.time_to_threshold import TimeToThreshold, \
    ttt_avg_agg, ttt_stats_agg
from user_metrics.metrics.edit_rate import EditRate, edit_rate_agg, \
    er_stats_agg
from user_metrics.metrics.namespace_of_edits import NamespaceEdits, \
    namespace_edits_sum
from user_metrics.metrics.live_account import LiveAccount, live_accounts_agg


# Registered metrics types
metric_dict =\
    {
    'threshold': Threshold,
    'survival': Survival,
    'revert_rate': RevertRate,
    'bytes_added': BytesAdded,
    'blocks': Blocks,
    'time_to_threshold': TimeToThreshold,
    'edit_rate': EditRate,
    'namespace_edits': NamespaceEdits,
    'live_account': LiveAccount,
    }

# @TODO: let metric types handle this mapping themselves and obsolete this
#            structure
aggregator_dict =\
    {
    'sum+bytes_added': ba_sum_agg,
    'mean+bytes_added': ba_mean_agg,
    'std+bytes_added': ba_std_agg,
    'sum+namespace_edits': namespace_edits_sum,
    'average+threshold': threshold_editors_agg,
    'average+survival': survival_editors_agg,
    'average+live_account': live_accounts_agg,
    'average+revert_rate': revert_rate_avg,
    'average+edit_rate': edit_rate_agg,
    'average+time_to_threshold': ttt_avg_agg,
    'median+bytes_added': ba_median_agg,
    'min+bytes_added': ba_min_agg,
    'max+bytes_added': ba_max_agg,
    'dist+edit_rate': er_stats_agg,
    'average+blocks': block_rate_agg,
    'dist+time_to_threshold': ttt_stats_agg,
    }


def get_metric_type(metric):
    return metric_dict[metric]


def get_aggregator_type(agg):
    try:
        return aggregator_dict[agg]
    except KeyError:
        raise MetricsAPIError(__name__ + ' :: Bad aggregator name.')


def get_metric_names():
    """ Returns the names of metric handles as defined by this module """
    return metric_dict.keys()


def get_aggregator_names():
    """ Returns the names of metric handles as defined by this module """
    return aggregator_dict.keys()


def get_param_types(metric_handle):
    """ Get the paramters for a given metric handle """
    return metric_dict[metric_handle]()._param_types


def get_agg_key(agg_handle, metric_handle):
    """ Compose the metric dependent aggregator handle """
    try:
        agg_key = '+'.join([agg_handle, metric_handle])
        if agg_key in aggregator_dict:
            return agg_key
        else:
            return ''
    except TypeError:
        return ''


# Define Types of requests handled by the manager
# ###############################################

# Enumeration to store request types
request_types = enum(time_series='time_series',
    aggregator='aggregator',
    raw='raw')


def get_request_type(request_meta):
    """ Determines request type. """
    if request_meta.aggregator and request_meta.time_series:
        return request_types.time_series
    elif request_meta.aggregator:
        return request_types.aggregator
    else:
        return request_types.raw


