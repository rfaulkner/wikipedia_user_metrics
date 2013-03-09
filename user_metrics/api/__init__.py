"""
    Metrics API module: http://metrics-api.wikimedia.org/

    Defines the API which exposes metrics on Wikipedia users.  The metrics
    are defined at https://meta.wikimedia.org/wiki/Research:Metrics.
"""


class MetricsAPIError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Error processing API request."):
        Exception.__init__(self, message)
