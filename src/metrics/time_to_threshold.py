

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import logging
from dateutil.parser import *
import user_metric as um

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class TimeToThreshold(um.UserMetric):
    """
        Produces an integer value representing the number of minutes taken to reach a threshold.

            `https://meta.wikimedia.org/wiki/Research:Metrics//time_to_threshold`

        usage e.g.: ::

            >>> import classes.Metrics as m
            >>> m.TimeToThreshold(m.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2).process(123456)
            500
    """

    def __init__(self,
                 threshold_type_class,
                 **kwargs):

        um.UserMetric.__init__(self, **kwargs)
        self._threshold_obj_ = threshold_type_class(**kwargs)

    def header(self):
        return self._threshold_obj_.header()

    def process(self, user_handle, is_id=True):
        self._results =  self._threshold_obj_.process(user_handle, self, is_id=is_id)
        return self

    class EditCountThreshold():
        """
            Nested Class. Objects of this class are to be created by the constructor of TimeToThreshold.  The class has one method,
            process(), which computes the time, in minutes, taken between making N edits and M edits for a user.  N < M.
        """

        def __init__(self, **kwargs):
            """
                Object constructor.  There are two required parameters:

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            try:
                self._first_edit_ = kwargs['first_edit']
                self._threshold_edit_ = kwargs['threshold_edit']

            except Exception:
                raise um.UserMetric.UserMetricError(str(self.__class__()) + ': Invalid init params.')

        def header(self):
            return []

        def process(self, user_handle, threshold_obj, is_id=True):
            """
                First determine if the user has made an adequate number of edits.  If so, compute the number of minutes that passed
                between the Nth and Mth edit.

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            minutes_to_threshold = list()

            # Operate on either user ids or names
            if is_id:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user = "%(user_handle)s" order by 1 desc'
            else:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user_text = "%(user_handle)s" order by 1 desc'

            if not isinstance(user_handle,list):
                user_handle = [user_handle]

            for user in user_handle:
                sql = user_revs_SQL % {'user_handle' : str(user), 'project' : threshold_obj._project_}
                minutes_to_threshold.append([user, self._get_minute_diff_result(threshold_obj._data_source_.execute_SQL(sql))])

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
                minutes_to_threshold = int(time_diff.seconds / 60) + abs(time_diff.days) * 24

            return minutes_to_threshold