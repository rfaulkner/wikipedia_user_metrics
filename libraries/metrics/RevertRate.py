

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import MySQLdb
import sys
import logging
import UserMetric as UM

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class RevertRate(UM.UserMetric):
    """
        Skeleton class for "RevertRate" metric:  `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate`
    """

    def __init__(self, **kwargs):
        UM.UserMetric.__init__(self, **kwargs)

    def process(self, user_handle, is_id=True):

        revert_rate = dict()
        return revert_rate
