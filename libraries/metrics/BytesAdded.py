
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import MySQLdb
import sys
import logging
import UserMetric as UM

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class BytesAdded(UM.UserMetric):
    """
        Produces a float value that reflects the rate of edit behaviour

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_rate`

            usage e.g.: ::

                >>> import classes.Metrics as M
                >>> M.BytesAdded(date_start='2012-07-30 00:00:00', raw_count=False, mode=1).process(123456)
                5
                1200
    """

    def __init__(self,
                 date_start='2001-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 raw_count=True,
                 project='enwiki',
                 **kwargs):
        """
            - Parameters:
                - **date_start**: string or datetime.datetime. start date of edit interval
                - **date_end**: string or datetime.datetime. end date of edit interval
                - **raw_count**: Boolean. Flag that when set to True returns one total count for all users.  Count by user otherwise.

            - Return:
                - Empty.
        """

        self._start_ts_ = self.get_timestamp(date_start)
        self._end_ts_ = self.get_timestamp(date_end)
        self.raw_count = raw_count
        self._project_ = project

        UM.UserMetric.__init__(self, **kwargs)

    def process(self, user_handle, is_id=True):
        """
            Determine the bytes added over a number of revisions for user(s).  The parameter *user_handle* can be either a string or an integer or a
            list of these types.  When the *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list
            of users is passed to the *process* method then a dict object with edit rates keyed by user handles is returned.

            The flow of the request is as follows:

                #. Get all revisions for the specified users in the given timeframe
                #. For each parent revision get its length
                #. Compute the difference in length between each revision and its parent
                #. Record edit count, raw bytes added (with sign and absolute), amount of positive bytes added, amount of negative bytes added

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

            - Return:
                - Dictionary. key(string): user handle, value(Float): edit counts
        """


        bytes_added = dict()
        edit_count = dict()
        ts_condition  = 'and rev_timestamp >= "%s" and rev_timestamp < "%s"' % (self._start_ts_, self._end_ts_)

        # Escape user_handle for SQL injection
        user_handle = self.escape_var(user_handle)

        if is_id:
            field_name = 'rev_user'
        else:
            field_name = 'rev_user_text'

        if isinstance(user_handle, list):
            # where_clause = DL.DataLoader().format_clause(user_handle,0,DL.DataLoader.OR,field_name)
            user_set = self._datasource_.format_comma_separated_list(user_handle)
            sql = 'select %(field_name)s, rev_len, rev_parent_id from %(project)s.revision where %(field_name)s in (%(user_set)s) %(ts_condition)s'
            sql = sql % {'field_name' : field_name, 'user_set' : user_set, 'ts_condition' : ts_condition, 'project' : self._project_}
        else:
            sql = 'select %(field_name)s, rev_len, rev_parent_id from %(project)s.revision where %(field_name)s = "%(user_handle)s" %(ts_condition)s'
            sql = sql % {'field_name' : field_name, 'user_handle' : str(user_handle), 'ts_condition' : ts_condition, 'project' : self._project_}

        # Process results and add to key-value
        results = list()
        try:
            results = self._datasource_.execute_SQL(sql)
        except:
            logging.error('Could not get bytes added for specified users(s).' )

        # Get the difference for each revision length from the parent to compute bytes added
        for row in results:

            try:
                user = row[0]
                rev_len_total = int(row[1])
                parent_rev_id = row[2]

                if not(user in bytes_added.keys()):
                    bytes_added[user] = [0,0,0,0,0]
                    edit_count[user] = 0
            except:
                logging.error('Could not retrieve results from row: %s' % str(row))
                continue

            try:
                # In case of a new article
                if parent_rev_id == 0:
                    parent_rev_len = 0
                else:
                    sql = 'select rev_len from enwiki.revision where rev_id = %(parent_rev_id)s' % {'parent_rev_id' : parent_rev_id}
                    parent_rev_len = int(self._datasource_.execute_SQL(sql)[0][0])
            except:
                logging.error('Could not produce rev diff for %s on rev_id %s.' % (user, str(parent_rev_id)))
                continue

            try:

                bytes_added_bit = rev_len_total - parent_rev_len

                bytes_added[user][0] += bytes_added_bit
                bytes_added[user][1] += abs(bytes_added_bit)
                if bytes_added_bit > 0:
                    bytes_added[user][2] += bytes_added_bit
                else:
                    bytes_added[user][3] += bytes_added_bit

                bytes_added[user][4] += 1

            except:
                logging.error('Could not perform bytes added calculation for user %s, rev_len_total: %s, parent_rev_len: %s' % (user, rev_len_total, parent_rev_len))

        # If a raw count has been flagged produce a sum of all
        if self.raw_count:
            total_bytes_added = [0,0,0,0,0]
            for user in bytes_added:
                for i in range(5):
                    total_bytes_added[i] += bytes_added[user][i]
            bytes_added = total_bytes_added

        return bytes_added