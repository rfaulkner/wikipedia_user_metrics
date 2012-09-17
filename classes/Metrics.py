"""

This module will be used to define WMF metrics.  The Template Method behavioural pattern (http://en.wikipedia.org/wiki/Template_method_pattern) will
be used to implement the metrics generation.  For example: ::

    class Metric(object):

        def __init__(self):
            # initialize base metric

            return

        def process(self):
            # base metric implementation

            return metric_value


    class DerivedMetric(Metric):

        def __init__(self):
            super(DerivedMetric, self)

            # initialize derived metric

            return

        def process(self):
            # derived metric implementation

            return metric_value


These metrics will be used to support experimentation and measurement at the Wikimedia Foundation.  The guidelines for this development may be found at
https://meta.wikimedia.org/wiki/Research:Metrics.

"""

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"

import classes.DataLoader as DL
import TimestampProcessor as TP
import datetime
import MySQLdb
import sys
import logging
from dateutil.parser import *

# CONFIGURE THE LOGGER
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class UserMetric(object):

    def __init__(self,
                 datasource=None,
                 project='enwiki',
                 namespace=0,
                 **kwargs):

        if not(isinstance(datasource, DL.DataLoader)):
            self._datasource_ = DL.DataLoader(db='db42')
        else:
            self._datasource_ = datasource

        self._namespace_ = namespace
        self._project_ = project



    def get_timestamp(self, ts_representation):
        """
            Helper method.  Takes a representation of a date object (String or datetime.datetime object) and formats
            as a timestamp: "YYYY-MM-DD HH:II:SS"

            - Parameters:
                - *date_representation* - String or datetime.  A formatted timestamp representation

            - Return:
                - String.  Timestamp derived from argument in format "YYYY-MM-DD HH:II:SS".
        """

        try:
            if isinstance(ts_representation, datetime.datetime):
                ts = str(ts_representation)[:19]
            elif isinstance(ts_representation, str):
                ts = str(parse(ts_representation))
            else:
                raise Exception()

            ts = TP.timestamp_convert_format(ts,2,1)

            return ts
        except:
            logging.info('Could not parse datetime: %s' % str(ts_representation))
            return None


    def escape_var(self, var):
        """
            Escapes either elements of a list (recursively visiting elements) or a single variable.  The variable
            is cast to string before being escaped.

            - Parameters:
                - **var**: List or string.  Variable or list (potentially nested) of variables to be escaped.

            - Return:
                - List or string.  escaped elements.
        """

        # If the input is a list recursively call on elements
        # TODO: potentailly extend for dictionaries
        if isinstance(var, list):
            escaped_var = list()
            for elem in var:
                escaped_var.append(self.escape_var(elem))
            return escaped_var
        else:
            return MySQLdb._mysql.escape_string(str(var))


    def process(self, user_handle, is_id=True):
        """

        """
        return 0


class EditCount(UserMetric):
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

        self._start_ts_ = self.get_timestamp(date_start)
        self._end_ts_ = self.get_timestamp(date_end)
        self.raw_count = raw_count

        UserMetric.__init__(self, **kwargs)



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
        user_handle = self.escape_var(user_handle)

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


class BytesAdded(UserMetric):
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

        UserMetric.__init__(self, **kwargs)

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


class EditRate(UserMetric):
    """
        Produces a float value that reflects the rate of edit behaviour

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_rate`

            usage e.g.: ::

                >>> import classes.Metrics as m
                >>> m.EditRate(date_start='2012-12-12 00:00:00',date_end='2012-12-12 00:00:00', namespace=3).process(123456)
                2.50
    """

    # Constants for denoting the time unit by which to normalize edit counts to produce an edit rate
    HOUR = 0
    DAY = 1

    def __init__(self,
                 time_unit_count=1,
                 time_unit=DAY,
                 date_start='2001-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 **kwargs):

        self._time_unit_count_ = time_unit_count
        self._time_unit_ = time_unit
        self._start_ts_ = self.get_timestamp(date_start)
        self._end_ts_ = self.get_timestamp(date_end)

        UserMetric.__init__(self, **kwargs)

    def process(self, user_handle, is_id=True):
        """
            Determine the edit rate of user(s).  The parameter *user_handle* can be either a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list of users is passed to the
            *process* method then a dict object with edit rates keyed by user handles is returned.

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

            - Return:
                - Dictionary. key(string): user handle, value(Float): edit counts
        """

        # Extract edit count for given parameters
        edit_rate = EditCount(date_start = self._start_ts_,
            date_end = self._end_ts_,
            datasource = self._datasource_,
            namespace=self._namespace_).process(user_handle, is_id=is_id)

        # Convert start and end times to objects, compute the difference
        if isinstance(self._start_ts_, string):
            start_ts_obj = parse(self._start_ts_)
        else:
            start_ts_obj = self._start_ts_

        if isinstance(self._end_ts_, string):
            end_ts_obj = parse(self._end_ts_)
        else:
            end_ts_obj = self._end_ts_

        time_diff_sec = (end_ts_obj - start_ts_obj).total_seconds()


        if self._time_unit_ == EditRate.DAY:
            time_diff = time_diff_sec / (24 * 60 * 60)

        elif self._time_unit_ == EditRate.HOUR:
            time_diff = time_diff_sec / (60 * 60)

        else:
            time_diff = time_diff_sec


        if isinstance(edit_rate, dict):
            for key in edit_rate:
                edit_rate[key] = edit_count[key] / (time_diff * self._time_unit_count_)
        else:
            edit_rate /= (time_diff * self._time_unit_count_)

        return edit_rate


class RevertRate(UserMetric):
    """
        `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate`
    """

    def __init__(self):
        UserMetric.__init__(self, **kwargs)

    def process(self, user_handle, is_id=True):
        return revert_rate


class TimeToThreshold(UserMetric):
    """
        Produces an integer value representing the number of minutes taken to reach a threshold.

            `https://meta.wikimedia.org/wiki/Research:Metrics//time_to_threshold`

        usage e.g.: ::

            >>> import classes.Metrics as m
            >>> m.TimeToThreshold(m.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2).process(123456)
            500
    """

    EDIT_COUNT_THRESHOLD = 0

    def __init__(self,
                 threshold_type,
                 **kwargs):

        UserMetric.__init__(self, **kwargs)

        if threshold_type == self.EDIT_COUNT_THRESHOLD:

            try:
                first_edit = kwargs['first_edit']
                threshold_edit = kwargs['threshold_edit']

                self._threshold_obj_ = TimeToThreshold.EditCountThreshold(first_edit, threshold_edit)

            except(Exception):
                logging.info('Could not instantiate EditCountThreshold object.')

    def process(self, user_handle, is_id=True):
        return self._threshold_obj_.process(user_handle, self, is_id=is_id)

    class EditCountThreshold():
        """
            Nested Class. Objects of this class are to be created by the constructor of TimeToThreshold.  The class has one method,
            process(), which computes the time, in minutes, taken between making N edits and M edits for a user.  N < M.
        """

        def __init__(self, first_edit, threshold_edit):
            """
                Object constructor.  There are two required parameters:

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            self._first_edit_ = first_edit
            self._threshold_edit_ = threshold_edit


        def process(self, user_handle, threshold_obj, is_id=True):
            """
                First determine if the user has made an adequate number of edits.  If so, compute the number of minutes that passed
                between the Nth and Mth edit.

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            minutes_to_threshold = list()

            if is_id:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user = "%(user_handle)s" order by 1 desc'
            else:
                user_revs_SQL = 'select rev_timestamp from %(project)s.revision where rev_user_text = "%(user_handle)s" order by 1 desc'

            if isinstance(user_handle,list):
                for user in user_handle:
                    sql = user_revs_SQL % {'user_handle' : str(user), 'project' : threshold_obj._project_}
                    minutes_to_threshold.append([user, self._get_minute_diff_result(threshold_obj._datasource_.execute_SQL(sql))])
            else:
                sql = user_revs_SQL % {'user_handle' : str(user_handle), 'project' : threshold_obj._project_}
                minutes_to_threshold.append([user_handle, self._get_minute_diff_result(threshold_obj._datasource_.execute_SQL(sql))])

            return minutes_to_threshold

        def _get_minute_diff_result(self, results):
            """
                Private method for this class.  This computes the minutes to threshold for the timestamp results.

                    - Parameters:
                        - **first_edit** - Integer.  The numeric value of the first edit from which to measure the threshold.
                        - **threshold_edit** - Integer.  The numeric value of the threshold edit from which to measure the threshold
            """

            if len(results) < self._threshold_edit_:
                return -1
            else:
                minutes_to_threshold = int((parse(results[self._threshold_edit_ - 1][0]) - parse(results[self._first_edit_ - 1][0])).seconds / 60)

            return minutes_to_threshold


class Retention(UserMetric):
    """
        Generates a set of retention flags and a day count of retention since a initiation event (registration, first successful edit).

            `https://meta.wikimedia.org/wiki/Research:Metrics/retention`

        usage e.g.: ::

            >>> import classes.Metrics as m
            >>> m.Retention(thresholds=[1,2,5,10]).process(123456)
            {'123456' : [{1 : True, 10 : False}, 7]}
    """

    def __init__(self, **kwargs):
        UserMetric.__init__(self, **kwargs)

    def process(self, user_handle, is_id=True):
        """
            Determine the retention rate of user(s).  Retention is indicated by a flag that measures whether a user reaches a time dependent threshold from a
            starting point (registration, first edit).  The parameter *user_handle* can be either a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list of users is passed to the
            *process* method then a dict object with edit rates keyed by user handles is returned.

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

            - Return:
                - Dictionary.
        """

        retention = dict()

        #
        retention_sql = ''
        self._datasource_.execute_SQL(retention_sql)

        return retention