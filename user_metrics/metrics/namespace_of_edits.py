
__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "January 6th, 2013"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

import user_metric as um
import user_metrics.utils.multiprocessing_wrapper as mpw
from collections import namedtuple, OrderedDict
from user_metrics.etl.aggregator import decorator_builder
from os import getpid
from user_metrics.metrics import query_mod
from user_metrics.metrics.users import UMP_MAP


class NamespaceEdits(um.UserMetric):
    """
        Skeleton class for "namespace of edits" metric:

            `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate`

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The
        execution of process() produces a nested list that stores in each
        element:

            * User ID
            * Dictionary of namespace edit counts

        For example to produce the above datapoint for a user id one could
        call: ::

            >>> from user_metrics.metrics.namespace_of_edits import
            NamespaceEdits
            >>> users = ['17792132', '17797320', '17792130', '17792131',
                            '17792136', 13234584, 156171]
            >>> n = NamespaceEdits(date_start='20110101000000')
            >>> for r in r.process(users, log=True): print r
            Jan-15 15:25:29 INFO     __main__::parameters = {'num_threads':
                1, 'log': True}
            Jan-15 15:25:30 INFO     __main__::Computing namespace edits.
                (PID = 20102)
            Jan-15 15:25:30 INFO     __main__::From 20110101000000 to
                20130115152529. (PID = 20102)
            ['namespace_edits_sum', OrderedDict([('-1', 0), ('-2', 0), ('0',
                227), ('1', 11), ('2', 158), ('3', 38), ('4', 578), ('5', 27),
                ('6', 1), ('7', 0), ('8', 0), ('9', 0), ('10', 13), ('11', 8),
                ('12', 0), ('13', 0), ('14', 5), ('15', 0), ('100', 1),
                ('101', 2), ('108', 0), ('109', 0)])]

    """

    # namespaces or which counts are gathered
    VALID_NAMESPACES = [-1, -2] + range(16) + [100, 101, 108, 109]

    # Structure that defines parameters for RevertRate class
    _param_types = {
        'init': {},
        'process': {}
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
        'namespace_edits_sum': _data_model_meta['integer_fields'] +
        _data_model_meta['float_fields'],
    }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(NamespaceEdits, self).__init__(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'revision_data_by_namespace', ]

    @um.UserMetric.pre_process_metric_call
    def process(self, user_handle, **kwargs):

        # ensure the handles are iterable
        if not hasattr(user_handle, '__iter__'):
            user_handle = [user_handle]

        # Multiprocessing vs. single processing execution
        args = self._pack_params()
        self._results = mpw.build_thread_pool(user_handle, _process_help,
                                              self.k_, args)
        return self


def _process_help(args):
    """
        Worker thread method for NamespaceOfEdits::process().
    """

    users = args[0]
    state = args[1]

    metric_params = um.UserMetric._unpack_params(state)
    query_args_type = namedtuple('QueryArgs', 'start end')

    if metric_params.log_:
        logging.info(__name__ + '::Computing namespace edits. (PID = %s)' %
                                getpid())

    # Tally counts of namespace edits
    results = dict()
    ump_res = UMP_MAP[metric_params.group](users, metric_params)
    for ump_rec in ump_res:

        results[str(ump_rec.user)] = OrderedDict()

        for ns in NamespaceEdits.VALID_NAMESPACES:
            results[str(ump_rec.user)][str(ns)] = 0

        query_results = query_mod.namespace_edits_rev_query([ump_rec.user],
            metric_params.project,
            query_args_type(ump_rec.start, ump_rec.end))

        for row in query_results:
            try:
                if row[1] in NamespaceEdits.VALID_NAMESPACES:
                    results[str(row[0])][str(row[1])] = int(row[2])
            except (KeyError, IndexError):
                logging.error(__name__ + "::Could not process row: %s" % str(row))
                continue

    return [(user, results[user]) for user in results]


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================
# @TODO use aggregator method


@decorator_builder(NamespaceEdits.header())
def namespace_edits_sum(metric):
    """ Computes the fraction of editors reaching a threshold """
    summed_results = ["namespace_edits_sum", OrderedDict()]
    for ns in NamespaceEdits.VALID_NAMESPACES:
        summed_results[1][str(ns)] = 0
    for r in metric.__iter__():
        try:
            for ns in NamespaceEdits.VALID_NAMESPACES:
                summed_results[1][str(ns)] += r[1][str(ns)]
        except (IndexError, TypeError):
            continue
    return summed_results
setattr(namespace_edits_sum, um.METRIC_AGG_METHOD_FLAG, True)
setattr(namespace_edits_sum, um.METRIC_AGG_METHOD_NAME,
        'namespace_edits_aggregates')
setattr(namespace_edits_sum, um.METRIC_AGG_METHOD_HEAD, ['type',
                                                         'total_revs',
                                                         'weighted_rate',
                                                         'total_editors',
                                                         'reverted_editors'])

if __name__ == "__main__":
    users = ['17792132', '17797320', '17792130', '17792131',
             '17792136', 13234584, 156171]

    m = NamespaceEdits(date_start='20110101000000')
    m.process(users, log=True)

    print namespace_edits_sum(m)
