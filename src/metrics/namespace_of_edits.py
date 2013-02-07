
__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "January 6th, 2013"
__license__ = "GPL (version 2 or later)"

import user_metric as um
import src.utils.multiprocessing_wrapper as mpw
from src.etl.data_loader import Connector, DataLoader
from collections import namedtuple, OrderedDict
from src.etl.aggregator import decorator_builder
from config import logging
from os import getpid

# Definition of persistent state for RevertRate objects
NamespaceEditsArgsClass = namedtuple('NamespaceEditsArgs', 'project log date_start date_end')

class NamespaceEdits(um.UserMetric):
    """
        Skeleton class for "namespace of edits" metric:

            `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Dictionary of namespace edit counts

        For example to produce the above datapoint for a user id one could call: ::

            >>> from src.metrics.namespace_of_edits import NamespaceEdits
            >>> users = ['17792132', '17797320', '17792130', '17792131', '17792136', 13234584, 156171]
            >>> n = NamespaceEdits(date_start='20110101000000')
            >>> for r in r.process(users, log=True): print r
            Jan-15 15:25:29 INFO     __main__::parameters = {'num_threads': 1, 'log': True}
            Jan-15 15:25:30 INFO     __main__::Computing namespace edits. (PID = 20102)
            Jan-15 15:25:30 INFO     __main__::From 20110101000000 to 20130115152529. (PID = 20102)
            ['namespace_edits_sum', OrderedDict([('-1', 0), ('-2', 0), ('0', 227), ('1', 11), ('2', 158), ('3', 38),
            ('4', 578), ('5', 27), ('6', 1), ('7', 0), ('8', 0), ('9', 0), ('10', 13), ('11', 8), ('12', 0), ('13', 0),
            ('14', 5), ('15', 0), ('100', 1), ('101', 2), ('108', 0), ('109', 0)])]

    """

    VALID_NAMESPACES = [-1,-2] + range(16) + [100, 101, 108, 109] # namespaces or which counts are gathered

    # Structure that defines parameters for RevertRate class
    _param_types = {
        'init' : {},
        'process' : {
            'log' : ['bool', 'Enable logging for processing.',False],
            'num_threads' : ['int', 'Number of worker processes over users.',1],
            }
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields' : [0],
        'date_fields' : [],
        'float_fields' : [1],
        'integer_fields' : [2],
        'boolean_fields' : [],
        }

    _agg_indices = {
        'namespace_edits_sum' : _data_model_meta['integer_fields'] + _data_model_meta['float_fields'],
        }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        um.UserMetric.__init__(self, **kwargs)

    @staticmethod
    def header(): return ['user_id', 'revision_data_by_namespace', ]

    @um.UserMetric.pre_process_users
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

    state = args[1]
    thread_args = NamespaceEditsArgsClass(state[0],state[1],state[2],state[3])
    user_data = args[0]
    conn = Connector(instance='slave')

    to_string = DataLoader().cast_elems_to_string
    to_csv_str = DataLoader().format_comma_separated_list

    # Format user condition
    user_cond = "rev_user in (" + to_csv_str(to_string(user_data)) + ")"

    # Format timestamp condition
    ts_cond = "rev_timestamp >= %s and rev_timestamp < %s" % (thread_args.date_start, thread_args.date_end)

    if thread_args.log:
        logging.info(__name__ + '::Computing namespace edits. (PID = %s)' % getpid())
        logging.info(__name__ + '::From %s to %s. (PID = %s)' % (
            str(thread_args.date_start), str(thread_args.date_end), getpid()))
    sql = """
            SELECT
                r.rev_user,
                p.page_namespace,
                count(*) AS revs
            FROM %(project)s.revision AS r JOIN %(project)s.page AS p
                ON r.rev_page = p.page_id
            WHERE %(user_cond)s AND %(ts_cond)s
            GROUP BY 1,2
        """ % {
        "user_cond" : user_cond,
        "ts_cond" : ts_cond,
        "project" : thread_args.project,
    }
    conn._cur_.execute(" ".join(sql.split('\n')))

    # Tally counts of namespace edits
    results = dict()

    for user in user_data:
        results[str(user)] = OrderedDict()
        for ns in NamespaceEdits.VALID_NAMESPACES: results[str(user)][str(ns)] = 0
    for row in conn._cur_:
        try:
            if row[1] in NamespaceEdits.VALID_NAMESPACES:
                results[str(row[0])][str(row[1])] = int(row[2])
        except KeyError:
            logging.error(__name__ + "::Could not process row: %s" % str(row))
            pass
        except IndexError:
            logging.error(__name__ + "::Could not process row: %s" % str(row))
            pass

    del conn
    return [(user, results[user]) for user in results]

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
        except IndexError: continue
        except TypeError: continue
    return summed_results
setattr(namespace_edits_sum, um.METRIC_AGG_METHOD_FLAG, True)
setattr(namespace_edits_sum, um.METRIC_AGG_METHOD_NAME, 'namespace_edits_aggregates')
setattr(namespace_edits_sum, um.METRIC_AGG_METHOD_HEAD, ['type', 'total_revs',
                                                       'weighted_rate','total_editors','reverted_editors'])

if __name__ == "__main__":
    users = ['17792132', '17797320', '17792130', '17792131', '17792136', 13234584, 156171]

    m = NamespaceEdits(date_start='20110101000000')
    m.process(users,log=True)
    # for r in NamespaceEdits(date_start='20110101000000').process(users,log=True): print r
    print namespace_edits_sum(m)