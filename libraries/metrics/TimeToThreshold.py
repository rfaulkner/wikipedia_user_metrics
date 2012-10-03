

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import MySQLdb
import sys
import logging
from dateutil.parser import *
import UserMetric as UM

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class TimeToThreshold(UM.UserMetric):
    """
        Produces an integer value representing the number of minutes taken to reach a threshold.

            `https://meta.wikimedia.org/wiki/Research:Metrics//time_to_threshold`

        usage e.g.: ::

            >>> import classes.Metrics as m
            >>> m.TimeToThreshold(m.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2).process(123456)
            500
    """

    EDIT_COUNT_THRESHOLD = 0

    def __init__(self,
                 threshold_type,
                 **kwargs):

        UM.UserMetric.__init__(self, **kwargs)

        if threshold_type == self.EDIT_COUNT_THRESHOLD:

            try:
                first_edit = kwargs['first_edit']
                threshold_edit = kwargs['threshold_edit']

                self._threshold_obj_ = TimeToThreshold.EditCountThreshold(first_edit, threshold_edit)

            except Exception:
                logging.info('Could not instantiate EditCountThreshold object.')

    def process(self, user_handle, is_id=True):
        return self._threshold_obj_.process(user_handle, self, is_id=is_id)

    class EditCountThreshold():
        """
            Nested Class. Objects of this class are to be created by the constructor of TimeToThreshold.  The class has one method,
            process(), which computes the time, in minutes, taken between making N edits and M edits for a user.  N < M.
        """

        def __init__(self, first_edit, threshold_edit):
            """
                Object constructor.  There are two required parameters:

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            self._first_edit_ = first_edit
            self._threshold_edit_ = threshold_edit


        def process(self, user_handle, threshold_obj, is_id=True):
            """
                First determine if the user has made an adequate number of edits.  If so, compute the number of minutes that passed
                between the Nth and Mth edit.

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            minutes_to_threshold = list()

            if is_id:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user = "%(user_handle)s" order by 1 desc'
            else:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user_text = "%(user_handle)s" order by 1 desc'

            if isinstance(user_handle,list):
                for user in user_handle:
                    sql = user_revs_SQL % {'user_handle' : str(user), 'project' : threshold_obj._project_}
                    minutes_to_threshold.append([user, self._get_minute_diff_result(threshold_obj._datasource_.execute_SQL(sql))])
            else:
                sql = user_revs_SQL % {'user_handle' : str(user_handle), 'project' : threshold_obj._project_}
                minutes_to_threshold.append([user_handle, self._get_minute_diff_result(threshold_obj._datasource_.execute_SQL(sql))])

            return minutes_to_threshold

        def _get_minute_diff_result(self, results):
            """
                Private method for this class.  This computes the minutes to threshold for the timestamp results.

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            if len(results) < self._threshold_edit_:
                return -1
            else:
                time_diff = parse(results[self._threshold_edit_ - 1][0]) - parse(results[self._first_edit_ - 1][0])
                minutes_to_threshold = int((time_diff).seconds / 60) + abs(time_diff.days) * 24

            return minutes_to_threshold