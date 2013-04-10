

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging
from os import getpid

from dateutil.parser import parse as date_parse
import user_metric as um
from user_metrics.etl.aggregator import weighted_rate, decorator_builder, \
    build_numpy_op_agg, build_agg_meta
from user_metrics.metrics import query_mod
from numpy import median, min, max
import user_metrics.utils.multiprocessing_wrapper as mpw

# Constants for threshold events
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
                'first_edit': [int,
                               'Event that initiates measurement period.',
                               REGISTRATION],
                'threshold_edit': [int, 'Threshold event.', 1],
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

    @staticmethod
    def header():
        return ['user_id', 'minutes_diff']

    @um.UserMetric.pre_process_metric_call
    def process(self, users, **kwargs):
        """ Wrapper for specific threshold objects """

        args = self._pack_params()
        self._results = mpw.build_thread_pool(users, _process_help,
                                              self.k_, args)

        return self


def _process_help(args):
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

    # Unpack args
    state = args[1]
    users = args[0]

    thread_args = um.UserMetric._unpack_params(state)

    if thread_args.log_:
        logging.debug(__name__ + '::Computing Time to threshold on '
                                 '{0} users. (PID = {1})'.format(len(users),
                                                                 getpid()))
    minutes_to_threshold = list()

    # For each user gather their revisions and produce a time diff
    for user in users:
        revs = query_mod.\
            time_to_threshold_revs_query(user, thread_args.project, None)
        revs = [rev[0] for rev in revs]
        minutes_to_threshold.append(
            [user, get_minute_diff_result(revs,
                                          thread_args.threshold_edit,
                                          thread_args.first_edit)])

    if thread_args.log_:
        logging.info(__name__ + '::Processed PID = {0}.'.format(getpid()))

    return minutes_to_threshold


def get_minute_diff_result(results, first, threshold):
    """
        Helper method.  This computes the minutes
        to threshold for the timestamp results.

            - Parameters:
                - **results** - list.  list of revision records with
                    timestamp for a given user.
    """
    if threshold == REGISTRATION and len(results):
        dat_obj_end = date_parse(results[0])
    elif threshold == LAST_EDIT and len(results):
        dat_obj_end = date_parse(results[len(results) - 1])
    elif threshold < len(results):
        dat_obj_end = date_parse(results[threshold])
    else:
        return -1

    if first == REGISTRATION and len(results) > 0:
        dat_obj_start = date_parse(results[0])
    elif first == LAST_EDIT and len(results):
        dat_obj_start = date_parse(results[len(results) - 1])
    elif first < len(results):
        dat_obj_start = date_parse(results[first])
    else:
        return -1

    time_diff = dat_obj_end - dat_obj_start
    return int(time_diff.seconds / 60) + abs(time_diff.days) * 24


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
