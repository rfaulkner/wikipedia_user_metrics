"""
    This module handles formatting standard API responses.
"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "2013-03-12"
__license__ = "GPL (version 2 or later)"

from datetime import datetime
from collections import OrderedDict

from user_metrics.api.engine import DATETIME_STR_FORMAT


def format_response(request):
    """
        Populates data for response to metrics requests.

        Parameters
        ~~~~~~~~~~

            request : RequestMeta
                RequestMeta object that stores request data.
    """
    response = OrderedDict()
    response['time_of_response'] = datetime.now().strftime(DATETIME_STR_FORMAT)
    response['cohort'] = str(request.cohort_expr)
    response['cohort_last_generated'] = str(request.cohort_gen_timestamp)
    response['aggregator'] = str(request.aggregator)
    return response