
__author__ = "Ryan Faulkner"
__date__ = "December 6th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import user_metric as um

class Survival(um.UserMetric):
    """
        Boolean measure of the retention of editors. Editors are considered "surviving" if they continue to participate
        after t minutes since an event (often, the user's registration or first edit).

            `https://meta.wikimedia.org/wiki/Research:Metrics/survival(t)`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Net bytes contributed over the period of measurement
            * Absolute bytes contributed over the period of measurement
            * Bytes added over the period of measurement

        usage e.g.: ::

            >>> import classes.Metrics as M
            >>> M.BytesAdded(date_start='2012-07-30 00:00:00', raw_count=False, mode=1).process(123456)
            5
            1200
    """

    def __init__(self,
                 date_start='2001-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 t=1440,
                 **kwargs):

        """
            - Parameters:
                - **date_start**: string or datetime.datetime. start date of edit interval
                - **date_end**: string or datetime.datetime. end date of edit interval
        """
        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)
        self._t_ = t
        um.UserMetric.__init__(self, **kwargs)

    @staticmethod
    def header():
        return ['user_id', 'is_', ]

    def process(self, user_handle, is_id=True, **kwargs):

        """
            Determine ...

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

        """

        return self