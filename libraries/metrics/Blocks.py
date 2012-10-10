
__author__ = "Ryan Faulkner and Aaron Halfaker"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import logging
import UserMetric as UM

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class Blocks(UM.UserMetric):
    """
        Adapted from Aaron Hafaker's implementation -- uses the logging table to count blocks.  This is a user quality
        metric used to assess whether a user went on to do damage.

        Example: ::

            >>> import libraries.metrics.Blocks as blocks
            >>> b = B.Blocks(date_start='2011-01-01 00:00:00')
            >>> p.process(['Wesley Mouse', 'Nickyp88'])
            {'Nickyp88': {'ban': -1, 'block': [1L, '20110809143215', '20110809143215']}, 'Wesley_Mouse': {'ban': -1, 'block': [2L, '20110830010835', '20120526192657']}}
    """
    def __init__(self,
                 date_start='2001-01-01 00:00:00',
                 wiki='enwiki',
                 return_list=False,
                 return_generator=True,
                 **kwargs):

        self._date_start_ = date_start
        self._wiki_ = wiki
        self._return_list_ = return_list
        self._return_generator_ = return_generator

        UM.UserMetric.__init__(self, **kwargs)


    def process(self, user_handle, is_id=False):
        """
            Process method for the "blocks" metric.  Returns a hash containing

            Parameters:
                - **elems** - List.  Elements to format as csv string
                - **include_quotes** - Boolean.  Determines whether the return string inserts quotes around the elements

            Return:
                - Dict().  {USER : {BLOCKS : [count, time of first, time of last]}, {BAN : time of ban}}

        """
        rowValues = {}

        if isinstance(user_handle, list):
            for i in xrange(len(user_handle)):
                try:
                    user_handle[i] = user_handle[i].encode('utf-8').replace(" ", "_")
                except UnicodeDecodeError:
                    user_handle[i] = user_handle[i].replace(" ", "_")
                rowValues[user_handle[i]] = {'block_count' : 0, 'block_first' : -1, 'block_last' : -1, 'ban' : -1}

            user_handle_str = self._datasource_.format_comma_separated_list(user_handle)
        else:
            try:
                user_handle = user_handle.encode('utf-8').replace(" ", "_")
            except UnicodeDecodeError:
                user_handle = user_handle.replace(" ", "_")

            rowValues[user_handle] = {'block_count' : 0, 'block_first' : -1, 'block_last' : -1, 'ban' : -1}
            user_handle_str = self._datasource_.format_comma_separated_list([user_handle])

        cursor = self._datasource_._cur_

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
				AND log_title in (%(usernames)s)
				GROUP BY 1, 2
			""" % {
            'timestamp': self._date_start_,
            'usernames': user_handle_str,
            'wiki' : self._wiki_
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

        if self._return_generator_:
            return ([user, rowValues.get(user)['block_count'], rowValues.get(user)['block_first'], rowValues.get(user)['block_last'], rowValues.get(user)['ban']] for user in rowValues.keys())
        elif self._return_list_:
            return [[user, rowValues.get(user)['block_count'], rowValues.get(user)['block_first'], rowValues.get(user)['block_last'], rowValues.get(user)['ban']] for user in rowValues.keys()]
        else:
            return rowValues
