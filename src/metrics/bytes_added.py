
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import user_metric as um

class BytesAdded(um.UserMetric):
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
        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)
        um.UserMetric.__init__(self, project=project, **kwargs)

    @staticmethod
    def header(): return ['user_id', 'bytes_added_net', 'bytes_added_absolute', 'bytes_added_pos', 'bytes_added_neg', 'edit_count']

    def process(self, user_handle=None, is_id=True):

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
        ts_condition  = 'rev_timestamp >= "%s" and rev_timestamp < "%s"' % (self._start_ts_, self._end_ts_)

        # determine the format field
        field_name = ['rev_user_text','rev_user'][is_id]

        # build the user set for inclusion into the query
        if not user_handle is None:
            user_handle = self._escape_var(user_handle) # Escape user_handle for SQL injection
            if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
            if is_id:
                user_set = um.dl.DataLoader().format_comma_separated_list(user_handle, include_quotes=False)
            else:
                user_set = um.dl.DataLoader().format_comma_separated_list(user_handle, include_quotes=True)
            where_clause = '%(field_name)s in (%(user_set)s) and %(ts_condition)s' % {
            'field_name' : field_name, 'user_set' : user_set, 'ts_condition' : ts_condition}
        else:
            where_clause = '%(ts_condition)s' % {'ts_condition' : ts_condition}

        sql = """
                select
                    %(field_name)s,
                    rev_len,
                    rev_parent_id
                from %(project)s.revision
                where %(where_clause)s
            """ % {
                'field_name' : field_name,
                'where_clause' : where_clause,
                'project' : self._project_}
        sql = " ".join(sql.strip().split())

        try:
            cur_1 = self._data_source_._db_.cursor()
            cur_1.execute(sql)
        except um.MySQLdb.ProgrammingError:
            raise self.UserMetricError(message=str(self.__class__()) +
                                               '::Could not get bytes added for specified users(s) - Query Failed.')

        # Get the difference for each revision length from the parent to compute bytes added
        cur_2 = self._data_source_._db_.cursor() # Get a new cursor for rev length queries
        for row in cur_1.fetchall():
            try:
                user = str(row[0])
                rev_len_total = int(row[1])
                parent_rev_id = row[2]

            except IndexError:
                continue

            # Produce the revision length of the parent
            if parent_rev_id == 0: # In case of a new article, parent_rev_id = 0, no record in the db
                parent_rev_len = 0
            else:
                sql = 'select rev_len from enwiki.revision where rev_id = %(parent_rev_id)s' % {
                      'parent_rev_id' : parent_rev_id}
                try:
                    cur_2.execute(sql)
                    parent_rev_len = int(cur_2.fetchall()[0][0])
                except um.MySQLdb.ProgrammingError:
                    raise um.UserMetric.UserMetricError(message=str(self.__class__()) +
                                '::Could not produce rev diff for %s on rev_id %s.' % (user, str(parent_rev_id)))

            # Update the bytes added hash
            bytes_added_bit = rev_len_total - parent_rev_len

            try: # Exception where the user does not exist.  Handle this by creating the key
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


        self._results = [[user] + bytes_added[user] for user in bytes_added]

        return self