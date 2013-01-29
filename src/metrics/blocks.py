
__author__ = "Ryan Faulkner and Aaron Halfaker"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import user_metric as um

from config import logging

class Blocks(um.UserMetric):
    """
        Adapted from Aaron Hafaker's implementation -- uses the logging table to count blocks.  This is a user quality
        metric used to assess whether a user went on to do damage.

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Block count
            * Date of first block
            * Date of Last block
            * Date of ban

        The process method for this metric breaks conformity with other metrics in that it expects usernames, and not
        user IDs, by default (see example below).

        Example: ::

            >>> import src.metrics.blocks as b
            >>> block_obj = b.Blocks(date_start='2011-01-01 00:00:00')
            >>> for r in block_obj.process(['11174885', '15132776']).__iter__(): print r
            ...
            ['15132776', 1L, '20110809143215', '20110809143215', -1]
            ['11174885', 2L, '20110830010835', '20120526192657', -1]
    """

    # Structure that defines parameters for Blocks class
    _param_types = {
        'init' : {},
        'process' : {
            'is_id' : ['bool', 'Are user ids or names being passed.', True],
            'log_progress' : ['bool', 'Enable logging for processing.',False],
        }
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields' : [0],
        'date_fields' : [2,3],
        'float_fields' : [],
        'integer_fields' : [1,4],
        'boolean_fields' : [],
        }

    _agg_indices = {
        'list_sum_indices' : _data_model_meta['integer_fields'] + _data_model_meta['float_fields'],
        }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        um.UserMetric.__init__(self, **kwargs)

    @staticmethod
    def header(): return ['user_id', 'block_count', 'block_first', 'block_last', 'ban']

    @um.UserMetric.pre_process_users
    def process(self, user_handle, **kwargs):
        """
            Process method for the "blocks" metric.  Computes a list of block and ban events for users.

            Parameters:
                - **user_handle** - List.  List of user IDs.
                - **is_id** - Boolean.  Defaults to False.

            Return:
                - UserMetric::Blocks (self).

        """

        self.apply_default_kwargs(kwargs,'process')
        rowValues = {}

        log = bool(kwargs['log_progress'])

        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
        users = um.dl.DataLoader().cast_elems_to_string(user_handle)

        for i in xrange(len(users)):
            rowValues[users[i]] = {'block_count' : 0, 'block_first' : -1, 'block_last' : -1, 'ban' : -1}

        cursor = self._data_source_._cur_
        user_dict = dict()

        # Get usernames for user ids to detect in block events
        users = um.dl.DataLoader().cast_elems_to_string(users)
        user_str = um.dl.DataLoader().format_comma_separated_list(users)
        cursor.execute('select user_id, user_name from enwiki.user where user_id in (%s)' % user_str)

        for r in cursor: user_dict[r[1]] = r[0] # keys username on userid
        user_handle_str = um.dl.DataLoader().format_comma_separated_list(user_dict.keys())

        # Get blocks from the logging table
        if log: logging.info(__name__ + '::Processing blocks for %s users.' % len(user_handle))
        sql = """
				SELECT
				    log_title as user,
					IF(log_params LIKE "%%indefinite%%", "ban", "block") as type,
					count(*) as count,
					min(log_timestamp) as first,
					max(log_timestamp) as last
				FROM %(wiki)s.logging
				WHERE log_type = "block"
				AND log_action = "block"
				AND log_title in (%(user_str)s)
				AND log_timestamp >= "%(timestamp)s"
				GROUP BY 1, 2
			""" % {
            'user_str' : user_handle_str,
            'timestamp': self._start_ts_,
            'user_cond': user_handle_str,
            'wiki' : self._project_
            }

        sql = " ".join(sql.strip().split())
        cursor.execute(sql)

        # Process rows - extract block and ban events
        for row in cursor:

            userid = str(user_dict[row[0]])
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

        self._results = [[user, rowValues.get(user)['block_count'], rowValues.get(user)['block_first'], rowValues.get(user)['block_last'], rowValues.get(user)['ban']] for user in rowValues.keys()]
        return self

if __name__ == "__main__":
    for r in Blocks().process(['11174885', '15132776']): print r