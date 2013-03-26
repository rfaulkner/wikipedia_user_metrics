"""
    Metrics API module: http://metrics-api.wikimedia.org/

    Defines the API which exposes metrics on Wikipedia users.  The metrics
    are defined at https://meta.wikimedia.org/wiki/Research:Metrics.
"""

# Error codes for web requests
# ############################

error_codes = {
    -1: 'Metrics API HTTP request error.',
    0: 'Job already running.',
    1: 'Badly Formatted timestamp',
    2: 'Could not locate stored request.',
    3: 'Could not find User ID.',
    4: 'Bad metric name.',
}


class MetricsAPIError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Error processing API request.",
                 error_code=-1):
        self.error_code_index = error_code
        Exception.__init__(self, message)

    @property
    def error_code(self):
        return self.error_code_index
