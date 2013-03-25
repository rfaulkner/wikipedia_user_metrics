
__author__ = "Ryan Faulkner and Aaron Halfaker"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

from collections import namedtuple
import user_metric as um
from user_metrics.metrics import query_mod
from user_metrics.etl.aggregator import weighted_rate, decorator_builder


class Blocks(um.UserMetric):
    """
        Adapted from Aaron Hafaker's implementation -- uses the logging table
        to count blocks.  This is a user quality metric used to assess whether
        a user went on to do damage.

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by user handle
        (typically ID but user names may also be specified). The execution of
        process() produces a nested list that stores in each element:

            * User ID
            * Block count
            * Date of first block
            * Date of Last block
            * Date of ban

        The process method for this metric breaks conformity with other
        metrics in that it expects usernames, and not user IDs, by default
        (see example below).

        Example: ::

            >>> import user_metrics.metrics.blocks as b
            >>> block_obj = b.Blocks(date_start='2011-01-01 00:00:00')
            >>> for r in block_obj.process(['11174885', '15132776']).
            __iter__(): print r
            ...
            ['15132776', 1L, '20110809143215', '20110809143215', -1]
            ['11174885', 2L, '20110830010835', '20120526192657', -1]
    """

    # Structure that defines parameters for Blocks class
    _param_types = \
        {
            'init': {},
            'process': {}
        }

    # Define the metrics data model meta
    _data_model_meta = \
        {
            'id_fields': [0],
            'date_fields': [2, 3],
            'float_fields': [],
            'integer_fields': [1, 4],
            'boolean_fields': [],
        }

    _agg_indices = \
        {
            'list_sum_indices': _data_model_meta['integer_fields'] +
            _data_model_meta['float_fields'],
        }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(Blocks, self).__init__(**kwargs)


    @staticmethod
    def header():
        return ['user_id',
                'block_count',
                'block_first',
                'block_last',
                'ban']

    @um.UserMetric.pre_process_metric_call
    def process(self, users, **kwargs):
        """
            Process method for the "blocks" metric.  Computes a list of
            block and ban events for users.

            Parameters:
                - **user_handle** - List.  List of user IDs.
                - **is_id** - Boolean.  Defaults to False.

            Return:
                - UserMetric::Blocks (self).

        """

        rowValues = {}

        for i in xrange(len(users)):
            rowValues[users[i]] = {'block_count': 0, 'block_first': -1,
                                   'block_last': -1, 'ban': -1}
        # Data calls
        user_map = query_mod.blocks_user_map_query(users, self.project)
        query_args = namedtuple('QueryArgs', 'date_start')(self.datetime_start)
        results = query_mod.blocks_user_query(users, self.project,
                                              query_args)

        # Process rows - extract block and ban events
        for row in results:

            userid = str(user_map[row[0]])
            type = row[1]
            count = row[2]
            first = row[3]
            last = row[4]

            if type == "block":
                rowValues[userid]['block_count'] = count
                rowValues[userid]['block_first'] = first
                rowValues[userid]['block_last'] = last

            elif type == "ban":
                rowValues[userid][type] = first

        self._results = [[user, rowValues.get(user)['block_count'],
                          rowValues.get(user)['block_first'],
                          rowValues.get(user)['block_last'],
                          rowValues.get(user)['ban']]
                         for user in rowValues.keys()]
        return self


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

# Build "rate" decorator
block_rate_agg = weighted_rate
block_rate_agg = decorator_builder(Blocks.header())(block_rate_agg)

setattr(block_rate_agg, um.METRIC_AGG_METHOD_FLAG, True)
setattr(block_rate_agg, um.METRIC_AGG_METHOD_NAME, 'b_rate_agg')
setattr(block_rate_agg, um.METRIC_AGG_METHOD_HEAD, ['total_users',
                                                    'total_weight',
                                                    'rate'])
setattr(block_rate_agg, um.METRIC_AGG_METHOD_KWARGS, {
    'val_idx': 1,
})


if __name__ == "__main__":
    for r in Blocks().process(['11174885', '15132776']):
        print r
