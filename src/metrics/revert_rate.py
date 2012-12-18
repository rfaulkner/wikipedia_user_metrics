

__author__ = "Ryan Faulkner (adapted from Aaron Halfaker's implementation)"
__date__ = "October 29th, 2012"
__license__ = "GPL (version 2 or later)"

import user_metric as um
import src.etl.data_loader as dl
import datetime
import collections
import os
import src.utils.multiprocessing_wrapper as mpw

# Definition of persistent state for RevertRate objects
RevertRateArgsClass = collections.namedtuple('RevertRateArgs', 'project log_progress '
                                                               'look_ahead look_back date_start date_end rev_threads')
class RevertRate(um.UserMetric):
    """
        Skeleton class for "RevertRate" metric:

            `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate`

        This metric computes how often a user has been reverted

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * Revert rate over the period of measurement
            * Total revisions over the period of measurement

        For example to produce the above datapoint for a user id one could call: ::

            >>> import src.metrics.revert_rate as rr
            >>>     r = RevertRate(date_start='2008-01-01 00:00:00', date_end='2008-05-01 00:00:00')
            >>> for r in r.process('156171',num_threads=0,rev_threads=10, log_progress=True): print r
            ['156171', 0.0, 210.0]

        In this call `look_ahead` and `look_back` indicate how many revisions in the past and in the future for a given
        article we are willing to look for a revert.  The identification of reverts is done by matching sha1 checksum values
        over revision history.
    """

    REV_SHA1_IDX = 2
    REV_USER_TEXT_IDX = 1

    def __init__(self,
                 look_back=15,
                 look_ahead=15,
                 date_start='2010-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 **kwargs):

        self.look_back = look_back
        self.look_ahead = look_ahead
        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)

        um.UserMetric.__init__(self, **kwargs)

    @staticmethod
    def header(): return ['user_id', 'revert_rate', 'total_revisions']

    def process(self, user_handle, is_id=True, **kwargs):

        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
        k = kwargs['num_threads'] if 'num_threads' in kwargs else 0
        k_r = kwargs['rev_threads'] if 'rev_threads' in kwargs else 1
        log_progress = bool(kwargs['log_progress']) if 'log_progress' in kwargs else False

        # Multiprocessing vs. single processing execution
        args = [self._project_, log_progress, self.look_ahead, self.look_back, self._start_ts_, self._end_ts_, k_r]
        if k:
            self._results = mpw.build_thread_pool(user_handle,_process_help,k,args)
        else:
            self._results = _process_help([user_handle, args])

        return self

def __revert(conn, rev_id, page_id, sha1, user_text, metric_args):
    """ Returns the revision corresponding to a revision if it exists. """
    history = {}
    for rev in __history(conn._db_, rev_id, page_id, metric_args.look_back, project=metric_args.project):
        history[rev[RevertRate.REV_SHA1_IDX]] = rev

    for rev in __future(conn._db_, rev_id, page_id, metric_args.look_ahead, project=metric_args.project):
        if rev[RevertRate.REV_SHA1_IDX] in history and rev[RevertRate.REV_SHA1_IDX] != sha1:
            if user_text == rev[RevertRate.REV_USER_TEXT_IDX]:
                return None
            else:
                return rev

def __history(conn, rev_id, page_id, n, project='enwiki'):
    """ Produce the n revisions on a page before a given revision """
    cursor = conn.cursor()
    cursor.execute(
        """
            SELECT rev_id, rev_user_text, rev_sha1
            FROM %(project)s.revision
            WHERE rev_page = %(page_id)s
                AND rev_id < %(rev_id)s
            ORDER BY rev_id DESC
            LIMIT %(n)s
        """ % {
            'rev_id':  rev_id,
            'page_id': page_id,
            'n':       n,
            'project': project
        }
    )

    for row in cursor:
        yield row

def __future(conn, rev_id, page_id, n, project='enwiki'):
    """ Produce the n revisions on a page after a given revision """
    cursor = conn.cursor()
    cursor.execute(
        """
            SELECT rev_id, rev_user_text, rev_sha1
            FROM %(project)s.revision
            WHERE rev_page = %(page_id)s
                AND rev_id > %(rev_id)s
            ORDER BY rev_id ASC
            LIMIT %(n)s
        """ % {
            'rev_id':  rev_id,
            'page_id': page_id,
            'n':       n,
            'project': project
        }
    )

    for row in cursor:
        yield row

def _process_help(args):
    """ Used by Threshold::process() for forking.  Should not be called externally. """

    state = args[1]
    thread_args = RevertRateArgsClass(state[0],state[1],state[2],state[3],state[4],state[5],state[6])
    user_data = args[0]
    conn = dl.Connector(instance='slave')

    if thread_args.log_progress:
        print str(datetime.datetime.now()) + ' - Computing reverts on %s users in thread %s.' % (len(user_data),
                                                                                            str(os.getpid()))
    results_agg = list()
    for user in user_data:
        conn._cur_.execute(
            """
           select
               rev_user,
               rev_page,
               rev_sha1,
               rev_user_text
           from %(project)s.revision
           where rev_user = %(user)s and
           rev_timestamp > "%(start_ts)s" and rev_timestamp <= "%(end_ts)s"
            """ % {
                'project' : thread_args.project,
                'user' : user,
                'start_ts' : thread_args.date_start,
                'end_ts' : thread_args.date_end
            })

        total_revisions = 0.0
        total_reverts = 0.0

        revisions = [rev for rev in conn._cur_]
        results_thread = mpw.build_thread_pool(revisions,_revision_proc,thread_args.rev_threads,state)

        for r in results_thread:
            total_revisions += r[0]
            total_reverts += r[1]

        if not total_revisions:
            results_agg.append([user, 0.0, total_revisions])
        else:
            results_agg.append([user, total_reverts / total_revisions, total_revisions])

    if thread_args.log_progress: print str(datetime.datetime.now()) +  'PID %s complete.' % (str(os.getpid()))
    return results_agg

def _revision_proc(args):
    """ helper method for computing reverts """

    state = args[1]
    thread_args = RevertRateArgsClass(state[0],state[1],state[2],state[3],state[4],state[5],state[6])
    rev_data = args[0]
    conn = dl.Connector(instance='slave')

    # if thread_args.log_progress: print 'Computing reverts on %s revisions in thread %s.' % (len(rev_data), str(os.getpid()))
    revision_count = 0.0
    revert_count = 0.0
    for rev in rev_data:
        if __revert(conn, rev[0], rev[1], rev[2], rev[3], thread_args):
            revert_count += 1.0
        revision_count += 1.0
    return [(revision_count, revert_count)]

# testing
if __name__ == "__main__":
    r = RevertRate()
    users = ['17792132', '17797320', '17792130', '17792131', '17792136',
             '17792137', '17792134', '17797328', '17797329', '17792138']
    for r in r.process(users,num_threads=2,rev_threads=10, log_progress=True): print r
    # for r in r.process('156171',num_threads=2,rev_threads=10, log_progress=True): print r
