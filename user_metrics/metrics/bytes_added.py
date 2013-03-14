
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

from numpy import median, min, max, mean, std
from collections import namedtuple
import user_metric as um
import os
from user_metrics.etl.aggregator import list_sum_by_group, \
    build_numpy_op_agg, build_agg_meta
import user_metrics.utils.multiprocessing_wrapper as mpw
from user_metrics.metrics import query_mod
from user_metrics.metrics.users import UMP_MAP


class BytesAdded(um.UserMetric):
    """
        Produces a float value that reflects the rate of edit behaviour:

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_rate`

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by user handle
        (typically ID but user names may also be specified). The execution of
        process() produces a nested list that stores in each element:

            * User ID
            * Net bytes contributed over the period of measurement
            * Absolute bytes contributed over the period of measurement
            * Bytes added over the period of measurement
            * Bytes removed over the period of measurement
            * Total edit count over the period of measurement

            usage e.g.: ::

                >>> import user_metrics.,metrics.bytes_added as ba
                >>> for r in ba.BytesAdded(date_start='2012-07-30 00:00:00').
                    process([13234584], num_threads=0).__iter__(): r
                ['13234584', 2, 2, 2, 0, 1]
                >>> ba.BytesAdded.header()
                ['user_id', 'bytes_added_net', 'bytes_added_absolute',
                    'bytes_added_pos', 'bytes_added_neg', 'edit_count']

        This metric forks a separate query on the revision table for each user
        specified in the call to process().  In order to optimize the
        execution of this implementation the call allows the caller to specify
        the number of threads as a keyword argument, `num_threads`, to the
        process() method.
    """

    # Structure that defines parameters for BytesAdded class
    _param_types = \
        {
            'init': {},
            'process': {}
        }

    # Define the metrics data model meta
    _data_model_meta = \
        {
            'id_fields': [0],
            'date_fields': [],
            'float_fields': [],
            'integer_fields': [1, 2, 3, 4, 5],
            'boolean_fields': [],
        }

    _agg_indices = \
        {
            'list_sum_indices': _data_model_meta['integer_fields'] +
            _data_model_meta['float_fields'],
        }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(BytesAdded, self).__init__(**kwargs)

    @staticmethod
    def header():

        return ['user_id', 'bytes_added_net', 'bytes_added_absolute',
                'bytes_added_pos', 'bytes_added_neg', 'edit_count']

    @um.UserMetric.pre_process_metric_call
    def process(self, users, **kwargs):
        """ Setup metrics gathering using multiprocessing """

        # get revisions
        args = self._pack_params()
        revs = mpw.build_thread_pool(users, _get_revisions, self.k_, args)

        # Start worker threads and aggregate results for bytes added

        self._results = \
            list_sum_by_group(mpw.build_thread_pool(revs,
                                                    _process_help,
                                                    self.k_,
                                                    args), 0)

        # Add any missing users - O(n)
        tallied_users = set([str(r[0]) for r in self._results])
        for user in users:
            if not tallied_users.__contains__(str(user)):
                # Add a row indicating no activity for that user
                self._results.append([user, 0, 0, 0, 0, 0])
        return self


def _get_revisions(args):
    """ Retrieve total set of revision records for users within timeframe """
    um.log_pool_worker_start(__name__, _get_revisions.__name__, args[0], args[1])

    users = args[0]
    state = args[1]

    metric_params = um.UserMetric._unpack_params(state)
    query_args_type = namedtuple('QueryArgs', 'date_start date_end namespace')

    revs = list()
    umpd_obj = UMP_MAP[metric_params.group](users, metric_params)
    try:
        for t in umpd_obj:
            revs += \
                list(query_mod.rev_query(t.user, metric_params.project,
                                         query_args_type(t.start, t.end,
                                                         metric_params.namespace)))
    except query_mod.UMQueryCallError as e:
        logging.error('{0}:: {1}. PID={2}'.format(__name__,
                                                  e.message, os.getpid()))
        return []

    um.log_pool_worker_end(__name__, _process_help.__name__)
    return revs


def _process_help(args):
    """
        Determine the bytes added over a number of revisions for user(s).  The
        parameter *user_handle* can be either a string or an integer or a list
        of these types.  When the *user_handle* type is integer it is
        interpreted as a user id, and as a user_name for string input.  If a
        list of users is passed to the *process* method then a dict object
        with edit rates keyed by user handles is returned.

        The flow of the request is as follows:

            #. Get all revisions for the specified users in the given
                timeframe
            #. For each parent revision get its length
            #. Compute the difference in length between each revision and its
                parent
            #. Record edit count, raw bytes added (with sign and absolute),
                amount of positive bytes added, amount of negative bytes added

        - Parameters:
            - **user_handle** - String or Integer (optionally lists).  Value
                or list of values representing user handle(s).
        - Return:
            - Dictionary. key(string): user handle, value(Float): edit counts
    """
    um.log_pool_worker_start(__name__, _process_help.__name__, args[0], args[1])

    revs = args[0]
    state = args[1]

    metric_params = um.UserMetric._unpack_params(state)
    bytes_added = dict()

    # Get the difference for each revision length from the parent
    # to compute bytes added
    row_count = 1
    missed_records = 0
    total_rows = len(revs)


    for row in revs:
        try:
            user = str(row[0])
            rev_len_total = int(row[1])
            parent_rev_id = row[2]

        except IndexError:
            missed_records += 1
            continue
        except TypeError:
            missed_records += 1
            continue

        # Produce the revision length of the parent.  In case of a new
        # article, parent_rev_id = 0, no record in the db
        if parent_rev_id == 0:
            parent_rev_len = 0
        else:
            try:
                parent_rev_len = query_mod.rev_len_query(parent_rev_id,
                                                         metric_params.project)
            except query_mod.UMQueryCallError:
                missed_records += 1
                logging.error(__name__ +
                              '::Could not produce rev diff for %s on '
                              'rev_id %s.' % (user, str(parent_rev_id)))
                continue

        # Update the bytes added hash - ignore revision if either rev length
        # is undetermined
        try:
            bytes_added_bit = int(rev_len_total) - int(parent_rev_len)
        except TypeError:
            missed_records += 1
            continue

        try:
            # Exception where the user does not exist.  Handle this by
            # creating the key
            bytes_added[user][0] += bytes_added_bit
        except KeyError:
            bytes_added[user] = [0] * 5
            bytes_added[user][0] += bytes_added_bit
            pass

        bytes_added[user][1] += abs(bytes_added_bit)
        if bytes_added_bit > 0:
            bytes_added[user][2] += bytes_added_bit
        else:
            bytes_added[user][3] += bytes_added_bit
        bytes_added[user][4] += 1

        row_count += 1

    results = [[user] + bytes_added[user] for user in bytes_added]

    extra = 'Processed {0} out of {1} records.'.\
        format(total_rows - missed_records, total_rows)
    um.log_pool_worker_end(__name__, _process_help.__name__, extra=extra)

    return results


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

metric_header = BytesAdded.header()

field_prefixes = {
    'net_': 1,
    'abs_': 2,
    'pos_': 3,
    'neg_': 4,
    'count_': 5,
}


# Build "mean" decorator
ba_mean_agg = build_numpy_op_agg(build_agg_meta([mean], field_prefixes),
                                   metric_header, 'ba_mean_agg')
# Build "standard deviation" decorator
ba_std_agg = build_numpy_op_agg(build_agg_meta([std], field_prefixes),
                                   metric_header, 'ba_std_agg')
# Build "sum" decorator
ba_sum_agg = build_numpy_op_agg(build_agg_meta([sum], field_prefixes),
                                  metric_header, 'ba_sum_agg')
# Build "median" decorator
ba_median_agg = build_numpy_op_agg(build_agg_meta([median], field_prefixes),
                                   metric_header, 'ba_median_agg')
# Build "min" decorator
ba_min_agg = build_numpy_op_agg(build_agg_meta([min], field_prefixes),
                                metric_header, 'ba_min_agg')
# Build "max" decorator
ba_max_agg = build_numpy_op_agg(build_agg_meta([max], field_prefixes),
                                metric_header, 'ba_max_agg')


# Used for testing
if __name__ == "__main__":
    #for r in
    # BytesAdded(date_start='20120101000000',date_end='20121101000000',
    # namespace=[0,1]).process(user_handle=['156171','13234584'],
    #    log_progress=True, num_threads=10): print r

    for r in BytesAdded(date_start='20120101000000', t=10000).\
        process(['156171', '13234584'],
                num_threads=10,
                log_=True):
        print r
