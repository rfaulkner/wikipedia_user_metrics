"""
    This module handles formatting standard API responses.
"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "2013-03-12"
__license__ = "GPL (version 2 or later)"

from datetime import datetime
from collections import OrderedDict
from dateutil.parser import parse as date_parse
from re import search

from user_metrics.api.engine import DATETIME_STR_FORMAT
from user_metrics.api.engine.request_meta import REQUEST_VALUE_MAPPING, \
    ParameterMapping, get_metric_type, get_request_type
from user_metrics.utils import reverse_dict

REVERSE_GROUP_MAP = reverse_dict(REQUEST_VALUE_MAPPING['group'])


def format_response(request):
    """
        Populates data for response to metrics requests.

        Parameters
        ~~~~~~~~~~

            request : RequestMeta
                RequestMeta object that stores request data.
    """

    args = ParameterMapping.map(request)

    metric_class = get_metric_type(request.metric)
    metric_obj = metric_class(**args)

    # Prepare metrics output for json response
    response = OrderedDict()

    response['type'] = get_request_type(request)
    response['header'] = metric_obj.header()

    # Get metric object params
    for key in metric_obj.__dict__:
        if not search(r'^_.*', key) and str(key) not in response:
            response[str(key)] = metric_obj.__dict__[key]

    response['cohort'] = str(request.cohort_expr)
    response['cohort_last_generated'] = str(request.cohort_gen_timestamp)
    response['time_of_response'] = datetime.now().strftime(DATETIME_STR_FORMAT)
    response['aggregator'] = str(request.aggregator)
    response['metric'] = str(request.metric)
    response['interval_hours'] = request.interval

    if request.group:
        response['group'] = REVERSE_GROUP_MAP[int(request.group)]
    else:
        # @TODO get access to the metric default for this attribute
        response['group'] = 'default'

    response['datetime_start'] = date_parse(metric_obj.datetime_start).\
        strftime(DATETIME_STR_FORMAT)
    response['datetime_end'] = date_parse(metric_obj.datetime_end).\
        strftime(DATETIME_STR_FORMAT)

    response['data'] = OrderedDict()

    return response, metric_class, metric_obj