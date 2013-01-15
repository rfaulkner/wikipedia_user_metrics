
__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "January 6th, 2013"
__license__ = "GPL (version 2 or later)"

import user_metric as um
import src.utils.multiprocessing_wrapper as mpw
from collections import namedtuple, OrderedDict
from config import logging
from os import getpid

# Definition of persistent state for RevertRate objects
LiveAccountArgsClass = namedtuple('LiveAccountArgs', 'project namespace log date_start date_end t')

class LiveAccount(um.UserMetric):
    """
        Skeleton class for "live account" metric:

            `https://meta.wikimedia.org/wiki/Research:Metrics/live_account`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * user ID
            * boolean value indicating whether the account is considered "live" given the parameters

        For example to produce the above datapoint for a user id one could call: ::

            >>> from src.metrics.live_account import LiveAccount
            >>> users = ['17792132', '17797320', '17792130', '17792131', '17792136', 13234584, 156171]
            >>> la = LiveAccount(date_start='20110101000000')
            >>> for r in r.process(users,log=True): print r

    """

    # Structure that defines parameters for RevertRate class
    _param_types = {
        'init' : {
            't' : ['int', 'The time in minutes until the threshold.', 60],
        },
        'process' : {
            'log' : ['bool', 'Enable logging for processing.',False],
            'num_threads' : ['int', 'Number of worker processes over users.',1],
            }
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields' : [0],
        'date_fields' : [],
        'float_fields' : [],
        'integer_fields' : [],
        'boolean_fields' : [1],
        }

    _agg_indices = {}

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        um.UserMetric.__init__(self, **kwargs)
        self._t_ = int(kwargs['t']) if 't' in kwargs else self._param_types['init']['t'][2]

    @staticmethod
    def header(): return ['user_id', 'is_active_account', ]

    def process(self, user_handle, **kwargs):

        self.apply_default_kwargs(kwargs,'process')

        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
        k = int(kwargs['num_threads'])
        log = bool(kwargs['log'])

        if log: logging.info(__name__ + "::parameters = " + str(kwargs))

        # Multiprocessing vs. single processing execution
        args = [self._project_, log, self._start_ts_, self._end_ts_]
        self._results = mpw.build_thread_pool(user_handle,_process_help,k,args)

        return self

def _process_help(args):
    return []

if __name__ == "__main__":
    users = ['17792132', '17797320', '17792130', '17792131', '17792136', 13234584, 156171]
    la = LiveAccount(date_start='20110101000000')
    for r in la.process(users,log=True): print r