

__author__ = "Ryan Faulkner (adapted from Aaron Halfaker's implementation)"
__date__ = "October 29th, 2012"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

from collections import namedtuple
import user_metric as um
import collections
import os
import user_metrics.utils.multiprocessing_wrapper as mpw
from user_metrics.etl.aggregator import decorator_builder, weighted_rate
from user_metrics.metrics import query_mod
from user_metrics.metrics.users import UMP_MAP
from user_metrics.utils import format_mediawiki_timestamp

# Definition of persistent state for RevertRate objects
RevertRateArgsClass = collections.namedtuple('RevertRateArgs',
                                             'project log_progress '
                                             'look_ahead look_back t '
                                             'rev_threads namespace '
                                             'group')


class RevertRate(um.UserMetric):
    """
        Skeleton class for "RevertRate" metric:

            `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate`

        This metric computes how often a user has been reverted

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified).
        The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Revert rate over the period of measurement
            * Total revisions over the period of measurement

        For example to produce the above datapoint for a user id one could
        call: ::

            >>> import user_metrics.metrics.revert_rate as rr
            >>>     r = RevertRate(date_start='2008-01-01 00:00:00',
                                    date_end='2008-05-01 00:00:00')
            >>> for r in r.process('156171',num_threads=0,rev_threads=10,
                                    log_progress=True): print r
            ['156171', 0.0, 210.0]

        In this call `look_ahead` and `look_back` indicate how many revisions
        in the past and in the future for a given article we are willing to
        look for a revert.  The identification of reverts is done by matching
        sha1 checksum values over revision history.
    """

    REV_SHA1_IDX = 2
    REV_USER_TEXT_IDX = 1

    # Structure that defines parameters for RevertRate class
    _param_types = {
        'init' : {
            'look_ahead': [int, 'Number of revisions to look '
                                'ahead when computing revert.', 15],
            'look_back': [int, 'Number of revisions to look '
                               'back when computing revert.', 15],
            't': [int, 'Length of measurement period.', 168],
        },
        'process' : {}
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields': [0],
        'date_fields': [],
        'float_fields': [1],
        'integer_fields': [2],
        'boolean_fields': [],
        }

    _agg_indices = {
        'list_sum_indices' : _data_model_meta['integer_fields'] +
                             _data_model_meta['float_fields'],
        }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(RevertRate, self).__init__(**kwargs)

    @staticmethod
    def header(): return ['user_id', 'revert_rate', 'total_revisions']

    @um.UserMetric.pre_process_metric_call
    def process(self, user_handle, **kwargs):

        # ensure the handles are iterable
        if not hasattr(user_handle, '__iter__'):
            user_handle = [user_handle]

        args = [self.project, self.log_, self.look_ahead,
                self.look_back, self.t, self.datetime_end, self.kr_,
                self.namespace, self.group]
        self._results = mpw.build_thread_pool(user_handle, _process_help,
                                              self.k_, args)

        return self


def __revert(rev_id, page_id, sha1, user_text, metric_args):
    """ Returns the revision corresponding to a revision if it exists. """
    history = {}
    for rev in __history(rev_id, page_id, metric_args.look_back,
                         metric_args.project, metric_args.namespace):
        history[rev[RevertRate.REV_SHA1_IDX]] = rev

    for rev in __future(rev_id, page_id, metric_args.look_ahead,
                        metric_args.project, metric_args.namespace):
        if rev[RevertRate.REV_SHA1_IDX] in history and \
           rev[RevertRate.REV_SHA1_IDX] != sha1:
            if user_text == rev[RevertRate.REV_USER_TEXT_IDX]:
                return None
            else:
                return rev


def __history(rev_id, page_id, n, project, namespace):
    """ Produce the n revisions on a page before a given revision
            Returns a generator of revision objects """
    return query_mod.page_rev_hist_query(rev_id, page_id, n, project,
                                                 namespace, look_ahead=False)


def __future(rev_id, page_id, n, project, namespace):
    """ Produce the n revisions on a page after a given revision
            Returns a generator of revision objects """
    return query_mod.page_rev_hist_query(rev_id, page_id, n, project,
                                                   namespace, look_ahead=True)


def _process_help(args):
    """ Used by Threshold::process() for forking.
        Should not be called externally. """

    state = args[1]
    thread_args = RevertRateArgsClass(state[0], state[1], state[2],
                                      state[3], state[4], state[6],
                                      state[7], state[8])
    users = args[0]

    if thread_args.log_progress:
        logging.info(__name__ +
                    ':: Computing reverts on %s users in thread %s.'
                    % (len(users), str(os.getpid())))
    results_agg = list()

    umpd_obj = UMP_MAP[thread_args.group](users, thread_args)
    for user_data in umpd_obj:

        total_revisions = 0.0
        total_reverts = 0.0

        # Call query on revert rate for each user
        #
        # 1. Obtain user registration date
        # 2. Compute end date based on 't'
        # 3. Get user revisions in time period
        query_args = namedtuple('QueryArgs', 'date_start date_end')\
            (format_mediawiki_timestamp(user_data.start),
             format_mediawiki_timestamp(user_data.end))
        revisions = query_mod.revert_rate_user_revs_query(user_data.user,
                                                          thread_args.project,
                                                          query_args)
        results_thread = mpw.build_thread_pool(revisions, _revision_proc,
                                               thread_args.rev_threads, state)

        for r in results_thread:
            total_revisions += r[0]
            total_reverts += r[1]
        if not total_revisions:
            results_agg.append([user_data.user, 0.0, total_revisions])
        else:
            results_agg.append([user_data.user, total_reverts / total_revisions,
                                total_revisions])

    if thread_args.log_progress: logging.info(__name__ +
                                              ':: PID %s complete.' %
                                              (str(os.getpid())))
    return results_agg


def _revision_proc(args):
    """ helper method for computing reverts """

    state = args[1]
    thread_args = RevertRateArgsClass(state[0], state[1], state[2],
                                      state[3], state[4], state[6],
                                      state[7], state[8])
    rev_data = args[0]

    revision_count = 0.0
    revert_count = 0.0
    for rev in rev_data:
        if __revert(rev[0], rev[1], rev[2], rev[3], thread_args):
            revert_count += 1.0
        revision_count += 1.0
    return [(revision_count, revert_count)]


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

# Build "weighted rate" decorator
revert_rate_avg = weighted_rate
revert_rate_avg = decorator_builder(RevertRate.header())(
                                    revert_rate_avg)

setattr(revert_rate_avg, um.METRIC_AGG_METHOD_FLAG, True)
setattr(revert_rate_avg, um.METRIC_AGG_METHOD_NAME, 'revert_rate_avg')
setattr(revert_rate_avg, um.METRIC_AGG_METHOD_HEAD, ['total_users',
                                                     'total_revisions',
                                                    'average_rate',])
setattr(revert_rate_avg, um.METRIC_AGG_METHOD_KWARGS, {'val_idx' : 1,
                                                       'weight_idx' : 1})

# testing
if __name__ == "__main__":
    r = RevertRate()
    users = ['17792132', '17797320', '17792130', '17792131', '17792136',
             '17792137', '17792134', '17797328', '17797329', '17792138']
    for i in xrange(5):
        users += users
    for r in r.process(users,num_threads=50,rev_threads=50, log_progress=True):
        print r


