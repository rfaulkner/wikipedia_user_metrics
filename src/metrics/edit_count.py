
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import MySQLdb
import sys
import logging
import user_metric as um

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class EditCount(um.UserMetric):
    """
        Produces a count of edits as well as the total number of bytes added for a registered user.

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_count(t)`

            usage e.g.: ::

                >>> import classes.Metrics as m
                >>> m.EditCount(date_start='2012-12-12 00:00:00',date_end='2012-12-12 00:00:00',namespace=0).process(123456)
                25, 10000

            The output in this case is the number of edits (25) made by the editor with ID 123456 and the total number of bytes added by those edits (10000)
    """

    def __init__(self,
                 date_start='2001-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 raw_count=True,
                 **kwargs):
        """
            Constructor for EditCount class.  Initialize timestamps over which metric is computed.

             - Paramters:
                - **date_start**: string or datetime.datetime. start date of edit interval
                - **date_end**: string or datetime.datetime. end date of edit interval
                - **raw_count**: Boolean. Flag that when set to True returns one total count for all users.  Count by user otherwise.
        """

        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)
        self.raw_count = raw_count

        um.UserMetric.__init__(self, **kwargs)



    def process(self, user_handle, is_id=True):
        """
            Determine edit count.  The parameter *user_handle* can be either a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list of users is passed
            to the *process* method then a dict object with edit counts keyed by user handles is returned.

            - Paramters:
                - **user_handle** - String or Integer (optionally lists):  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids
                - **raw_count** - Boolean.  Flag indicating whether to return simply an integer count or counts broken out by user handle

            - Return:
                - Dictionary. key(string): user handle, value(Integer): edit counts
        """

        if self.raw_count:
            edit_count = 0
        else:
            edit_count = dict()

        ts_condition  = 'and rev_timestamp >= "%s" and rev_timestamp < "%s"' % (self._start_ts_, self._end_ts_)

        # Escape user_handle for SQL injection
        user_handle = self._escape_var(user_handle)

        if is_id:
            field_name = 'rev_user'
        else:
            field_name = 'rev_user_text'

        if isinstance(user_handle, list):
            # where_clause = DL.DataLoader().format_clause(user_handle,0,DL.DataLoader.OR,field_name)
            user_set = self._datasource_.format_comma_separated_list(user_handle)
            sql = 'select %(field_name)s, count(*), sum(rev_len) from %(project)s.revision where %(field_name)s in (%(user_set)s) %(ts_condition)s group by 1'
            sql = sql % {'field_name' : field_name, 'user_set' : user_set, 'ts_condition' : ts_condition, 'project' : self._project_}
        else:
            sql = 'select %(field_name)s, count(*), sum(rev_len) from %(project)s.revision where %(field_name)s = "%(user_handle)s" %(ts_condition)s group by 1'
            sql = sql % {'field_name' : field_name, 'user_handle' : str(user_handle), 'ts_condition' : ts_condition, 'project' : self._project_}

        # Process results and add to key-value
        try:
            results = self._datasource_.execute_SQL(sql)

            for row in results:
                if self.raw_count:
                    edit_count = edit_count + int(row[1])
                else:
                    edit_count[row[0]] = [int(row[1]), int(row[2])]
        except:
            logging.info('Could not get edit count for %s' % str(user_handle))

        return edit_count
