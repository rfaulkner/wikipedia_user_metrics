
__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "January 6th, 2013"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

import user_metric as um
import user_metrics.utils.multiprocessing_wrapper as mpw
from collections import namedtuple
from os import getpid
from dateutil.parser import parse as date_parse
from user_metrics.etl.aggregator import decorator_builder, boolean_rate
from user_metrics.metrics import query_mod

# Definition of persistent state for RevertRate objects
LiveAccountArgsClass = namedtuple('LiveAccountArgs',
                                  'project namespace log '
                                  'date_start date_end t')


class LiveAccount(um.UserMetric):
    """
        Skeleton class for "live account" metric:

            `https://meta.wikimedia.org/wiki/Research:Metrics/live_account`

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by user handle
        (typically ID but user names may also be specified). The execution
        of process() produces a nested list that
        stores in each element:

            * user ID
            * boolean value indicating whether the account is considered
                "live" given the parameters

        For example to produce the above datapoint for a user id one could
        call: ::

            >>> from user_metrics.metrics.live_account import LiveAccount
            >>> users = ['17792132', '17797320', '17792130', '17792131',
                        '17792136', 13234584, 156171]
            >>> la = LiveAccount(date_start='20110101000000')
            >>> for r in r.process(users,log=True): print r
            ('17792130', -1)
            ('17792131', -1)
            ('17792132', -1)
            ('17797320', -1)
            ('156171', -1)
            ('17792136', 1)
            ('13234584', -1)

        Here the follow outcomes may follow: ::

            -1  - The edit button was not clicked after registration
            0   - The edit button was clicked more than `t` minutes
                    after registration
            1   - The edit button was clicked `t` minutes within registration
    """

    # Structure that defines parameters for RevertRate class
    _param_types = {
        'init': {
            't': ['int', 'The time in minutes until the threshold.', 60],
        },
        'process': {}
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields': [0],
        'date_fields': [],
        'float_fields': [],
        'integer_fields': [],
        'boolean_fields': [1],
    }

    _agg_indices = {}

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(LiveAccount, self).__init__(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'is_active_account', ]

    @um.UserMetric.pre_process_metric_call
    def process(self, user_handle, **kwargs):

        # Multiprocessing vs. single processing execution
        args = [self.project, self.namespace, self.log_, self.datetime_start,
                self.datetime_end, self.t]
        self._results = mpw.build_thread_pool(user_handle, _process_help,
                                              self.k_, args)
        return self


def _process_help(args):

    # Unpack args
    state = args[1]
    thread_args = LiveAccountArgsClass(state[0], state[1], state[2], state[3],
                                       state[4], state[5])
    users = args[0]

    # Log progress
    if thread_args.log:
        logging.debug(__name__ + '::Computing live account. (PID = %s)' %
                                 getpid())

    # Extract edit button click from edit_page_tracking table (namespace,
    # article title, timestamp) of first click and registration timestamps
    # (join on logging table)
    #
    # Query will return: (user id, time of registration, time of first
    # edit button click)
    query_args = namedtuple('QueryArgs', 'namespace')(thread_args.namespace)
    query_results = query_mod.live_account_query(users, thread_args.project,
                                                 query_args)

    # Iterate over results to determine boolean indicating whether
    # account is "live"
    results = {long(user): -1 for user in users}
    for row in query_results:
        try:
            # get the difference in minutes
            diff = (date_parse(row[2]) - date_parse(row[1])).total_seconds()
            diff /= 60
        except Exception:
            continue

        if diff <= thread_args.t:
            results[row[0]] = 1
        else:
            results[row[0]] = 0

    return [(str(key), results[key]) for key in results]


@decorator_builder(LiveAccount.header())
def live_accounts_agg(metric):
    """ Computes the fraction of editors that have "live" accounts """
    total = 0
    pos = 0
    for r in metric.__iter__():
        try:
            if r[1]:
                pos += 1
            total += 1
        except (IndexError, TypeError):
            continue
    if total:
        return [total, pos, float(pos) / total]
    else:
        return [total, pos, 0.0]

# Build "rate" decorator
live_accounts_agg = boolean_rate
live_accounts_agg = decorator_builder(LiveAccount.header())(live_accounts_agg)

setattr(live_accounts_agg, um.METRIC_AGG_METHOD_FLAG, True)
setattr(live_accounts_agg, um.METRIC_AGG_METHOD_NAME, 'live_accounts_agg')
setattr(live_accounts_agg, um.METRIC_AGG_METHOD_HEAD, ['total_users',
                                                       'is_live', 'rate'])
setattr(live_accounts_agg, um.METRIC_AGG_METHOD_KWARGS, {'val_idx': 1})

if __name__ == "__main__":
    users = ['17792132', '17797320', '17792130', '17792131', '17792136',
             13234584, 156171]
    la = LiveAccount()
    for r in la.process(users, log=True):
        print r
