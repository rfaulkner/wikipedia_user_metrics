
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import MySQLdb
import sys
import logging
from dateutil.parser import *
import user_metric as um
import edit_count as ec

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class EditRate(um.UserMetric):
    """
        Produces a float value that reflects the rate of edit behaviour

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_rate`

            usage e.g.: ::

                >>> import classes.Metrics as m
                >>> m.EditRate(date_start='2012-12-12 00:00:00',date_end='2012-12-12 00:00:00', namespace=3).process(123456)
                2.50
    """

    # Constants for denoting the time unit by which to normalize edit counts to produce an edit rate
    HOUR = 0
    DAY = 1

    def __init__(self,
                 time_unit_count=1,
                 time_unit=DAY,
                 date_start='2001-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 **kwargs):

        self._time_unit_count_ = time_unit_count
        self._time_unit_ = time_unit
        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)

        um.UserMetric.__init__(self, **kwargs)

    def process(self, user_handle, is_id=True):
        """
            Determine the edit rate of user(s).  The parameter *user_handle* can be either a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list of users is passed to the
            *process* method then a dict object with edit rates keyed by user handles is returned.

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

            - Return:
                - Dictionary. key(string): user handle, value(Float): edit counts
        """

        # Extract edit count for given parameters
        edit_rate = ec.EditCount(date_start = self._start_ts_,
            date_end = self._end_ts_,
            datasource = self._datasource_,
            namespace=self._namespace_).process(user_handle, is_id=is_id)

        # Convert start and end times to objects, compute the difference
        if isinstance(self._start_ts_, str):
            start_ts_obj = parse(self._start_ts_)
        else:
            start_ts_obj = self._start_ts_

        if isinstance(self._end_ts_, str):
            end_ts_obj = parse(self._end_ts_)
        else:
            end_ts_obj = self._end_ts_

        # Compute time difference between datetime objects and get the integer number of seconds
        time_diff_sec = (end_ts_obj - start_ts_obj).total_seconds()


        if self._time_unit_ == EditRate.DAY:
            time_diff = time_diff_sec / (24 * 60 * 60)

        elif self._time_unit_ == EditRate.HOUR:
            time_diff = time_diff_sec / (60 * 60)

        else:
            time_diff = time_diff_sec


        if isinstance(edit_rate, dict):
            for key in edit_rate:
                edit_rate[key] = edit_rate[key] / (time_diff * self._time_unit_count_)
        else:
            edit_rate /= (time_diff * self._time_unit_count_)

        return edit_rate

