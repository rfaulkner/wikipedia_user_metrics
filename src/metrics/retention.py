

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import MySQLdb
import sys
import logging
import user_metric as um

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class Retention(um.UserMetric):
    """
        Generates a set of retention flags and a day count of retention since a initiation event (registration, first successful edit).

            `https://meta.wikimedia.org/wiki/Research:Metrics/retention`

        usage e.g.: ::

            >>> import classes.Metrics as m
            >>> m.Retention(thresholds=[1,2,5,10]).process(123456)
            {'123456' : [{1 : True, 10 : False}, 7]}
    """

    def __init__(self, **kwargs):
        um.UserMetric.__init__(self, **kwargs)

    def __str__(self):
        """ Return a miniumum desrciption of the object """


        return

    def process(self, user_handle, is_id=True):
        """
            Determine the retention rate of user(s).  Retention is indicated by a flag that measures whether a user reaches a time dependent threshold from a
            starting point (registration, first edit).  The parameter *user_handle* can be either a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list of users is passed to the
            *process* method then a dict object with edit rates keyed by user handles is returned.

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

            - Return:
                - Dictionary.
        """

        retention = dict()

        # Get
        retention_sql = ''
        self._data_source_.execute_SQL(retention_sql)

        return retention


    class RetentionEvent():

        def __init__(self):
            return

