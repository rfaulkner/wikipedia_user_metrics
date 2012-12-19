
__author__ = "Ryan Faulkner and Aaron Halfaker"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import user_metric as um

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
            >>> for r in block_obj.process(['Wesley Mouse', 'Nickyp88']).__iter__(): print r
            ...
            ['Nickyp88', 1L, '20110809143215', '20110809143215', -1]
            ['Wesley_Mouse', 2L, '20110830010835', '20120526192657', -1]
    """

    # Structure that defines parameters for Blocks class
    _param_types = {
        'init' : {
            'date_start' : 'str', # earliest date a block is measured
        },
        'process' : {
            'is_id' : 'bool'
        }
    }

    def __init__(self,
                 date_start='2012-01-01 00:00:00',
                 project='enwiki',
                 **kwargs):

        self._date_start_ = um.UserMetric._get_timestamp(date_start)
        self._project_ = project
        um.UserMetric.__init__(self, project=project, **kwargs)
        self.append_params(um.UserMetric)   # add params from base class

    @staticmethod
    def header(): return ['user_id', 'block_count', 'block_first', 'block_last', 'ban']

    def process(self, user_handle, is_id=True, **kwargs):
        """
            Process method for the "blocks" metric.  Computes a list of block and ban events for users.

            Parameters:
                - **user_handle** - List.  List of user names (or IDs).
                - **is_id** - Boolean.  Defaults to False.

            Return:
                - UserMetric::Blocks (self).

        """
        rowValues = {}

        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
        for i in xrange(len(user_handle)):
            if hasattr(user_handle[i], 'encode'):
                try:
                    user_handle[i] = user_handle[i].encode('utf-8').replace(" ", "_")
                except UnicodeDecodeError:
                    user_handle[i] = user_handle[i].replace(" ", "_")
            rowValues[user_handle[i]] = {'block_count' : 0, 'block_first' : -1, 'block_last' : -1, 'ban' : -1}

        # Prepare condition on users based on whether numeric IDs or string usernames are passed
        if is_id:
            user_handle = um.dl.DataLoader().cast_elems_to_string(user_handle)
            user_handle_str = 'AND log_user in (' + um.dl.DataLoader().format_comma_separated_list(
                user_handle, include_quotes=False) + ')'
        else:
            user_handle_str = 'AND log_title in (' + um.dl.DataLoader().format_comma_separated_list(user_handle) + ')'

        cursor = self._data_source_._cur_
        sql = """
				SELECT
				    log_title as user_name,
					IF(log_params LIKE "%%indefinite%%", "ban", "block") as type,
					count(*) as count,
					min(log_timestamp) as first,
					max(log_timestamp) as last
				FROM %(wiki)s.logging
				WHERE log_type = "block"
				AND log_action = "block"
				%(user_cond)s
				AND log_timestamp >= "%(timestamp)s"
				GROUP BY 1, 2
			""" % {
            'timestamp': self._date_start_,
            'user_cond': user_handle_str,
            'wiki' : self._project_
            }

        sql = " ".join(sql.strip().split())
        cursor.execute(sql)

        for row in cursor:

            username = row[0]
            type = row[1]
            count = row[2]
            first = row[3]
            last = row[4]

            if type == "block":
                rowValues[username]['block_count'] = count
                rowValues[username]['block_first'] = first
                rowValues[username]['block_last'] = last

            elif type == "ban":
                rowValues[username][type] = first

        self._results = [[user, rowValues.get(user)['block_count'], rowValues.get(user)['block_first'], rowValues.get(user)['block_last'], rowValues.get(user)['ban']] for user in rowValues.keys()]
        return self