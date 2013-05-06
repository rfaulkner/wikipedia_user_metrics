
__author__ = {'Evan Rosen': 'erosen@wikimedia.org'}
__date__ = "April 14th, 2013"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

import os
from numpy import mean, std, median
import user_metrics.utils.multiprocessing_wrapper as mpw
import user_metric as um
from user_metrics.metrics import query_mod
from user_metrics.metrics.users import UMP_MAP


class PagesCreated(um.UserMetric):
    """
    Skeleton class for "PagesCreated" metric:

        `https://meta.wikimedia.org/wiki/Research:Metrics/pages_created`

    This metric computes how often a user has been reverted

    As a UserMetric type this class utilizes the process() function
    attribute to produce an internal list of metrics by
    user handle (typically ID but user names may also be specified).
    The execution of process() produces a nested list that
    stores in each element:

        * User ID
        * Number of pages created by user
    """

    # Structure that defines parameters for Threshold class
    _param_types = {
        'init': {},
        'process': {}
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
        'list_sum_indices': _data_model_meta['integer_fields'],
    }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(PagesCreated, self).__init__(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'pages_created']

    @um.UserMetric.pre_process_metric_call
    def process(self, users, **kwargs):

        # Process results
        args = self._pack_params()
        self._results = mpw.build_thread_pool(users, _process_help,
                                              self.k_, args)
        return self


def _process_help(args):
    """ Used by Threshold::process() for forking.
        Should not be called externally. """

    # Unpack args
    users = args[0]
    state = args[1]

    metric_params = um.UserMetric._unpack_params(state)

    if metric_params.log_:
        logging.info(__name__ + ' :: Processing pages created data ' +
                                '(%s users) by user... (PID = %s)' % (
                                    len(users), os.getpid()))
        logging.info(__name__ + ' :: ' + str(metric_params))

    # only proceed if there is user data
    if not len(users):
        return []

    results = list()
    dropped_users = 0
    umpd_obj = UMP_MAP[metric_params.group](users, metric_params)
    for t in umpd_obj:
        uid = long(t.user)
        try:
            count = query_mod.pages_created_query(uid,
                                                  metric_params.project,
                                                  metric_params)
            print count
        except query_mod.UMQueryCallError:
            dropped_users += 1
            continue

        try:
            results.append((str(uid), count[0][0]))
        except TypeError:
            dropped_users += 1

    if metric_params.log_:
        logging.info(__name__ + '::Processed PID = %s.  '
                                'Dropped users = %s.' % (
                                    os.getpid(), str(dropped_users)))

    return results


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

from user_metrics.etl.aggregator import build_numpy_op_agg, build_agg_meta
from user_metrics.metrics.user_metric import METRIC_AGG_METHOD_KWARGS

metric_header = PagesCreated.header()

field_prefixes =\
    {
        'count_': 1,
    }

# Build "dist" decorator
op_list = [sum, mean, std, median, min, max]
pages_created_stats_agg = build_numpy_op_agg(
    build_agg_meta(op_list, field_prefixes), metric_header,
    'pages_created_stats_agg')

agg_kwargs = getattr(pages_created_stats_agg, METRIC_AGG_METHOD_KWARGS)
setattr(pages_created_stats_agg, METRIC_AGG_METHOD_KWARGS, agg_kwargs)
