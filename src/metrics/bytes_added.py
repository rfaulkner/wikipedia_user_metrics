
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import collections
import datetime
import user_metric as um
import os
import src.etl.aggregator as agg
import src.utils.multiprocessing_wrapper as mpw

class BytesAdded(um.UserMetric):
    """
        Produces a float value that reflects the rate of edit behaviour:

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_rate`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Net bytes contributed over the period of measurement
            * Absolute bytes contributed over the period of measurement
            * Bytes added over the period of measurement
            * Bytes removed over the period of measurement
            * Total edit count over the period of measurement

            usage e.g.: ::

                >>> import src.,metrics.bytes_added as ba
                >>> for r in ba.BytesAdded(date_start='2012-07-30 00:00:00').process([13234584], num_threads=0).__iter__(): r
                ['13234584', 2, 2, 2, 0, 1]
                >>> ba.BytesAdded.header()
                ['user_id', 'bytes_added_net', 'bytes_added_absolute', 'bytes_added_pos', 'bytes_added_neg', 'edit_count']

        This metric forks a separate query on the revision table for each user specified in the call to process().  In order to
        optimize the execution of this implementation the call allows the caller to specify the number of threads as a keyword
        argument, `num_threads`, to the process() method.
    """

    # Structure that defines parameters for BytesAdded class
    _param_types = {
        'init' : {
            'date_start' : ['str|datetime', 'Earliest date a block is measured.', '2010-01-01 00:00:00'],
            'date_end' : ['str|datetime', 'Latest date a block is measured.', datetime.datetime.now()],
        },
        'process' : {
            'is_id' : ['bool', 'Are user ids or names being passed.', True],
            'log_progress' : ['bool', 'Enable logging for processing.', False],
            'log_frequency' : ['int', 'Revision frequency on which to log (ie. log every n revisions)', 1000],
            'num_threads' : ['int',   'Number of worker processes.', 0]
        }
    }

    @um.pre_metrics_init
    def __init__(self, **kwargs):

        um.UserMetric.__init__(self, **kwargs)

        self._start_ts_ = self._get_timestamp(kwargs['date_start'])
        self._end_ts_ = self._get_timestamp(kwargs['date_end'])

    @staticmethod
    def header(): return ['user_id', 'bytes_added_net', 'bytes_added_absolute',
                          'bytes_added_pos', 'bytes_added_neg', 'edit_count']

    @um.aggregator
    def process(self, user_handle, **kwargs):
        """ Setup metrics gathering using multiprocessing """

        self.apply_default_kwargs(kwargs,'process')

        is_id = kwargs['is_id']
        k = kwargs['num_threads']
        log_progress = bool(kwargs['log_progress'])
        log_frequency = int(kwargs['log_frequency'])

        if user_handle:
            if not hasattr(user_handle, '__iter__'): user_handle = [user_handle]

        # Multiprocessing vs. single processing execution
        revs = self._get_revisions(user_handle, is_id, log_progress=log_progress)
        args = [log_progress, log_frequency]
        if k:
            # Start worker threads and aggregate results
            self._results = agg.list_sum_by_group(mpw.build_thread_pool(revs,_process_help,k,args),0)
        else:
            self._results = _process_help([revs, args])

        return self

    def _get_revisions(self, user_handle, is_id, log_progress=True):

        # build the argument lists for each thread
        if not user_handle:
            sql = 'select distinct rev_user from enwiki.revision where rev_timestamp >= "%s" and rev_timestamp < "%s"'
            sql = sql % (self._start_ts_, self._end_ts_)
            print str(datetime.datetime.now()) + ' - Getting all distinct users: " %s "' % sql
            user_handle = [str(row[0]) for row in self._data_source_.execute_SQL(sql)]
            print str(datetime.datetime.now()) + ' - Retrieved %s users.' % len(user_handle)

        ts_condition  = 'rev_timestamp >= "%s" and rev_timestamp < "%s"' % (self._start_ts_, self._end_ts_)

        # determine the format field
        field_name = ['rev_user_text','rev_user'][is_id]

        # build the user set for inclusion into the query -
        # if the user_handle is empty or None get all users for timeframe
        user_handle = um.UserMetric._escape_var(user_handle) # Escape user_handle for SQL injection
        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
        if is_id:
            user_set = um.dl.DataLoader().format_comma_separated_list(user_handle, include_quotes=False)
        else:
            user_set = um.dl.DataLoader().format_comma_separated_list(user_handle, include_quotes=True)
        where_clause = '%(field_name)s in (%(user_set)s) and %(ts_condition)s' % {
            'field_name' : field_name, 'user_set' : user_set, 'ts_condition' : ts_condition}

        # format the namespace condition
        ns_cond = um.UserMetric._format_namespace(self._namespace_)
        if ns_cond: ns_cond += ' and'

        sql = """
                select
                    %(field_name)s,
                    rev_len,
                    rev_parent_id
                from %(project)s.revision
                    join %(project)s.page
                    on page.page_id = revision.rev_page
                where %(namespace)s %(where_clause)s
            """ % {
            'field_name' : field_name,
            'where_clause' : where_clause,
            'project' : self._project_,
            'namespace' : ns_cond}
        sql = " ".join(sql.strip().split())

        if log_progress:
            print str(datetime.datetime.now()) +\
                  ' - Querying revisions for %(count)s users (project = %(project)s, namespace = %(namespace)s)... ' % {
                      'count' : len(user_handle), 'project' : self._project_, 'namespace' : self._namespace_}
        try:
            return self._data_source_.execute_SQL(sql)
        except um.MySQLdb.ProgrammingError:
           raise um.UserMetric.UserMetricError(message=str(BytesAdded) +
                                                    '::Could not get revisions for specified users(s) - Query Failed.')

# Define the metrics data model meta
BytesAdded._data_model_meta['id_fields'] = [0]
BytesAdded._data_model_meta['float_fields'] = []
BytesAdded._data_model_meta['integer_fields'] = [1,2,3,4,5]


def _process_help(args):

    """
        Determine the bytes added over a number of revisions for user(s).  The parameter *user_handle* can
        be either a string or an integer or a list of these types.  When the *user_handle* type is integer it is
        interpreted as a user id, and as a user_name for string input.  If a list of users is passed to the
        *process* method then a dict object with edit rates keyed by user handles is returned.

        The flow of the request is as follows:

            #. Get all revisions for the specified users in the given timeframe
            #. For each parent revision get its length
            #. Compute the difference in length between each revision and its parent
            #. Record edit count, raw bytes added (with sign and absolute), amount of positive bytes added,
            amount of negative bytes added

        - Parameters:
            - **user_handle** - String or Integer (optionally lists).  Value or list of values representing
                                user handle(s).
            - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids

        - Return:
            - Dictionary. key(string): user handle, value(Float): edit counts
    """

    BytesAddedArgsClass = collections.namedtuple('ThresholdArgs', 'is_log freq')
    revs = args[0]
    state = args[1]
    thread_args = BytesAddedArgsClass(state[0],state[1])

    conn = um.dl.Connector(instance='slave')
    bytes_added = dict()

    # Get the difference for each revision length from the parent to compute bytes added
    row_count = 1
    missed_records = 0
    total_rows = len(revs)

    if thread_args.is_log:
        s = ' - Processing revision data (%s rows) by user... (PID = %s)' % (total_rows, os.getpid())
        print str(datetime.datetime.now()) + s

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

        # Produce the revision length of the parent
        if parent_rev_id == 0: # In case of a new article, parent_rev_id = 0, no record in the db
            parent_rev_len = 0
        else:
            sql = 'select rev_len from enwiki.revision where rev_id = %(parent_rev_id)s' % {
                  'parent_rev_id' : parent_rev_id}
            try:
                parent_rev_len = conn.execute_SQL(sql)[0][0]
            except IndexError:
                missed_records += 1
                continue
            except TypeError:
                missed_records += 1
                continue
            except um.MySQLdb.ProgrammingError:
                raise um.UserMetric.UserMetricError(message=str(BytesAdded) +
                            '::Could not produce rev diff for %s on rev_id %s.' % (user, str(parent_rev_id)))

        # Update the bytes added hash - ignore revision if either rev length is undetermined
        try:
            bytes_added_bit = int(rev_len_total) - int(parent_rev_len)
        except TypeError:
            missed_records += 1
            continue

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


        if thread_args.freq and row_count % thread_args.freq == 0 and thread_args.is_log:
            s = ' - Processed %s of %s records. (PID = %s)' % (row_count, total_rows, os.getpid())
            print str(datetime.datetime.now()) + s

        row_count += 1

    results = [[user] + bytes_added[user] for user in bytes_added]
    if thread_args.is_log:
        s = ' - processed %s out of %s records. (PID = %s)' % (total_rows - missed_records,total_rows, os.getpid())
        print str(datetime.datetime.now()) + s

    return results

# Used for testing
if __name__ == "__main__":
    #for r in BytesAdded(date_start='20120101000000',date_end='20121101000000', namespace=[0,1]).process(user_handle=['156171','13234584'],
    #    log_progress=True, num_threads=10): print r

    for r in BytesAdded().process(user_handle=['156171','13234584'], num_threads=10, log_progress=True): print r