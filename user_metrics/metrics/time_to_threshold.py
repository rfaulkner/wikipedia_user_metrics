

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

from dateutil.parser import parse as date_parse
import user_metric as um
from user_metrics.etl.aggregator import weighted_rate, decorator_builder, \
    build_numpy_op_agg, build_agg_meta
from user_metrics.metrics import query_mod
from numpy import median, min, max

LAST_EDIT = -1
REGISTRATION = 0


class TimeToThreshold(um.UserMetric):
    """
        Produces an integer value representing the number of minutes taken to
        reach a threshold.

          `https://meta.wikimedia.org/wiki/Research:Metrics/time_to_threshold`

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by user handle
        (typically ID but user names may also be specified). The execution of
        process() produces a nested list that stores in each element:

            * User ID
            * Time difference in minutes between events

        Below is an example of how we can generate the time taken to reach the
        first edit and the last edit from registration : ::

            >>> import user_metrics.metrics.time_to_threshold as t
            >>> t.TimeToThreshold(t.TimeToThreshold.EditCountThreshold,
                first_edit=0, threshold_edit=1).process([13234584]).
                    __iter__().next()
            [13234584, 3318]
            >>> t.TimeToThreshold(t.TimeToThreshold.EditCountThreshold,
                first_edit=0, threshold_edit=-1).process([13234584]).
                    __iter__().next()
            [13234584, 18906]

        If the termination event never occurs the number of minutes returned
        is -1.
    """

    # Structure that defines parameters for TimeToThreshold class
    _param_types = {
        'init':
        {
            'threshold_type_class': [str, 'Type of threshold to use.',
                                     'edit_count_threshold'],
        },
        'process': {},
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields': [0],
        'date_fields': [],
        'float_fields': [],
        'integer_fields': [1],
        'boolean_fields': [],
    }

    _agg_indices = {
        'list_sum_indices': _data_model_meta['integer_fields'] +
        _data_model_meta['float_fields'],
    }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(TimeToThreshold, self).__init__(**kwargs)

        try:
            self._threshold_obj_ = self.__threshold_types[
                self.threshold_type_class](**kwargs)
        except NameError:
            logging.error(__name__ + '::Invalid threshold class. '
                                     'Using default (EditCountThreshold).')
            self._threshold_obj_ = self.EditCountThreshold(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'minutes_diff']

    @um.UserMetric.pre_process_metric_call
    def process(self, users, **kwargs):
        """ Wrapper for specific threshold objects """
        self._results = self._threshold_obj_.process(users, self,
                                                     **kwargs)
        return self

    class EditCountThreshold():
        """
            Nested Class. Objects of this class are to be created by the
            constructor of TimeToThreshold.  The class has one method,
            process(), which computes the time, in minutes, taken between
            making N edits and M edits for a user.  N < M.
        """

        # Structure that defines parameters for TimeToThreshold class
        _param_types = {
            'init': {
                'first_edit': ['int',
                               'Event that initiates measurement period.',
                               REGISTRATION],
                'threshold_edit': ['int', 'Threshold event.', 1],
            },
            'process': {}
        }

        def __init__(self, **kwargs):
            """
                Object constructor.  There are two required parameters:

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the
                            first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of
                            the threshold edit from which to measure the
                            threshold
            """

            try:
                self._first_edit_ = int(kwargs['first_edit']) if 'first_edit' \
                    in kwargs else self._param_types['init']['first_edit'][2]

                self._threshold_edit_ = int(kwargs['threshold_edit']) if \
                    'threshold_edit' in kwargs else self._param_types[
                        'init']['threshold_edit'][2]

            except ValueError:
                raise um.UserMetricError(
                    str(self.__class__()) + ': Invalid init params.')

        def process(self, users, threshold_obj, **kwargs):
            """
                First determine if the user has made an adequate number of
                edits.  If so, compute the number of minutes that passed
                between the Nth and Mth edit.

                    - Parameters:
                        - **user_handle** - List(int).  List of user ids.
                        - **first_edit** - Integer.  The numeric value of
                            the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of
                            the threshold edit from which to measure the
                            threshold
            """

            minutes_to_threshold = list()

            # For each user gather their revisions
            for user in users:
                revs = query_mod.\
                    time_to_threshold_revs_query(user, threshold_obj.project,
                                                 None)
                revs = [rev[0] for rev in revs]
                minutes_to_threshold.append(
                    [user, self._get_minute_diff_result(revs)])

            return minutes_to_threshold

        def _get_minute_diff_result(self, results):
            """
                Private method for this class.  This computes the minutes
                to threshold for the timestamp results.

                    - Parameters:
                        - **results** - list.  list of revision records with
                            timestamp for a given user.
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

    __threshold_types = {'edit_count_threshold': EditCountThreshold}


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

# Build "average" aggregator
ttt_avg_agg = weighted_rate
ttt_avg_agg = decorator_builder(TimeToThreshold.header())(ttt_avg_agg)

setattr(ttt_avg_agg, um.METRIC_AGG_METHOD_FLAG, True)
setattr(ttt_avg_agg, um.METRIC_AGG_METHOD_NAME, 'ttt_avg_agg')
setattr(ttt_avg_agg, um.METRIC_AGG_METHOD_HEAD, ['total_users',
                                                 'total_weight',
                                                 'average'])
setattr(ttt_avg_agg, um.METRIC_AGG_METHOD_KWARGS, {'val_idx': 1})


metric_header = TimeToThreshold.header()

field_prefixes = {
    'time_diff_': 1,
}

# Build "dist" decorator
op_list = [median, min, max]
ttt_stats_agg = build_numpy_op_agg(build_agg_meta(op_list, field_prefixes),
                                   metric_header,
                                   'ttt_stats_agg')


if __name__ == "__main__":
    for i in TimeToThreshold(threshold_type_class='edit_count_threshold',
                             first_edit=0,
                             threshold_edit=1).process([13234584]):
        print i
