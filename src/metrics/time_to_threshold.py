

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from dateutil.parser import parse as date_parse
import datetime
import user_metric as um



LAST_EDIT = -1
REGISTRATION = 0

class TimeToThreshold(um.UserMetric):
    """
        Produces an integer value representing the number of minutes taken to reach a threshold.

            `https://meta.wikimedia.org/wiki/Research:Metrics//time_to_threshold`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Time difference in minutes between events

        Below is an example of how we can generate the time taken to reach the first edit and the last edit from registration : ::

            >>> import src.metrics.time_to_threshold as t
            >>> t.TimeToThreshold(t.TimeToThreshold.EditCountThreshold, first_edit=0, threshold_edit=1).process([13234584]).__iter__().next()
            [13234584, 3318]
            >>> t.TimeToThreshold(t.TimeToThreshold.EditCountThreshold, first_edit=0, threshold_edit=-1).process([13234584]).__iter__().next()
            [13234584, 18906]

        If the termination event never occurs the number of minutes returned is -1.
    """

    # Structure that defines parameters for TimeToThreshold class
    _param_types = {
        'init' : {
            'threshold_type_class' : ['str', 'Type of threshold to use.'],
            },
        'process' : {
            'is_id' : ['bool', 'Are user ids or names being passed.'],
        }
    }

    def __init__(self,
                 threshold_type_class=None,
                 **kwargs):

        if not threshold_type_class:
             self._threshold_obj_ = self.EditCountThreshold(**kwargs)
        else:
            try:
                self._threshold_obj_ = self.__threshold_types[threshold_type_class](**kwargs)
            except NameError:
                self._threshold_obj_ = self.EditCountThreshold(**kwargs)
                print str(datetime.datetime.now()) + ' - Invalid threshold class. Using default (EditCountThreshold).'

        um.UserMetric.__init__(self, **kwargs)
        self.append_params(um.UserMetric)   # add params from base class
        self._param_types['threshold_type_params'] = self._threshold_obj_._param_types

    @staticmethod
    def header(): return ['user_id', 'minutes_diff']

    def process(self, user_handle, is_id=True, **kwargs):
        """ Wrapper for specific threshold objects """
        self._results =  self._threshold_obj_.process(user_handle, self, is_id=is_id, **kwargs)
        return self

    class EditCountThreshold():
        """
            Nested Class. Objects of this class are to be created by the constructor of TimeToThreshold.  The class has one method,
            process(), which computes the time, in minutes, taken between making N edits and M edits for a user.  N < M.
        """

        # Structure that defines parameters for TimeToThreshold class
        _param_types = {
            'init' : {
                'first_edit' : ['int', 'Event that initiates measurement period.'],
                'threshold_edit' : ['int', 'Threshold event.'],
                },
            'process' : {
                'is_id' : ['bool', 'Are user ids or names being passed.'],
                }
        }

        def __init__(self, first_edit=REGISTRATION, threshold_edit=1):
            """
                Object constructor.  There are two required parameters:

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            try:
                self._first_edit_ = int(first_edit)
                self._threshold_edit_ = int(threshold_edit)

            except ValueError:
                raise um.UserMetric.UserMetricError(str(self.__class__()) + ': Invalid init params.')

        def process(self, user_handle, threshold_obj, is_id=True, **kwargs):
            """
                First determine if the user has made an adequate number of edits.  If so, compute the number of minutes that passed
                between the Nth and Mth edit.

                    - Parameters:
                        - **user_handle** - List(int).  List of user ids.
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            minutes_to_threshold = list()

            # Operate on either user ids or names
            if is_id:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user = "%(user_handle)s" order by 1 asc'
            else:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user_text = "%(user_handle)s" order by 1 asc'

            if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable

            # For each user gather their revisions
            for user in user_handle:
                sql = user_revs_SQL % {'user_handle' : str(user), 'project' : threshold_obj._project_}
                revs = [rev[0] for rev in threshold_obj._data_source_.execute_SQL(sql)]
                minutes_to_threshold.append([user, self._get_minute_diff_result(revs)])

            return minutes_to_threshold

        def _get_minute_diff_result(self, results):
            """
                Private method for this class.  This computes the minutes to threshold for the timestamp results.

                    - Parameters:
                        - **results** - list.  list of revision records with timestamp for a given user.
            """
            if self._threshold_edit_ == REGISTRATION and len(results):
                dat_obj_end = date_parse(results[0])
            elif self._threshold_edit_ == LAST_EDIT and len(results):
                dat_obj_end = date_parse(results[len(results) - 1])
            elif self._threshold_edit_ < len(results):
                dat_obj_end = date_parse(results[self._threshold_edit_])
            else:
                return -1

            if self._first_edit_ == REGISTRATION and len(results) > 0:
                dat_obj_start = date_parse(results[0])
            elif self._first_edit_ == LAST_EDIT and len(results):
                dat_obj_start = date_parse(results[len(results) - 1])
            elif self._first_edit_ < len(results):
                dat_obj_start = date_parse(results[self._first_edit_])
            else:
                return -1

            time_diff = dat_obj_end - dat_obj_start
            return int(time_diff.seconds / 60) + abs(time_diff.days) * 24

    __threshold_types = { 'edit_count_threshold' : EditCountThreshold }