

__author__ = "Ryan Faulkner (adapted from Aaron Halfaker's implementation)"
__date__ = "October 29th, 2012"
__license__ = "GPL (version 2 or later)"

import user_metric as um
import datetime

class RevertRate(um.UserMetric):
    """ Skeleton class for "RevertRate" metric:  `https://meta.wikimedia.org/wiki/Research:Metrics/revert_rate` """

    REV_SHA1_IDX = 2
    REV_USER_TEXT_IDX = 1

    def __init__(self,
                 look_back=15,
                 look_ahead=15,
                 date_start='2008-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 **kwargs):

        self.look_back = look_back
        self.look_ahead = look_ahead
        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)

        um.UserMetric.__init__(self, **kwargs)

    @staticmethod
    def header(): return ['user_id', 'revert_rate', 'total_revisions']

    def process(self, user_handle, is_id=True):

        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable

        # Get user revisions
        for user in user_handle:
            self._data_source_._cur_.execute(
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
                'project' : self._project_,
                'user' : user,
                'start_ts' : self._start_ts_,
                'end_ts' : self._end_ts_
                })

            total_revisions = 0.0
            total_reverts = 0.0
            for rev in self._data_source_._cur_:
                if self.__revert(rev[0], rev[1], rev[2], rev[3]):
                    total_reverts += 1.0
                total_revisions += 1.0

            if not total_revisions:
                self._results.append([user, 0.0, total_revisions])
            else:
                self._results.append([user, total_reverts / total_revisions, total_revisions])

        return self

    def __revert(self, rev_id, page_id, sha1, user_text):
        history = {}
        for rev in self.__history(self._data_source_._db_, rev_id, page_id, self.look_back, project=self._project_):
            history[rev[self.REV_SHA1_IDX]] = rev

        for rev in self.__future(self._data_source_._db_, rev_id, page_id, self.look_ahead, project=self._project_):
            if rev[self.REV_SHA1_IDX] in history and rev[self.REV_SHA1_IDX] != sha1:
                if user_text == rev[self.REV_USER_TEXT_IDX]:
                    return None
                else:
                    return rev

    @staticmethod
    def __history(conn, rev_id, page_id, n, project='enwiki'):
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

    @staticmethod
    def __future(conn, rev_id, page_id, n, project='enwiki'):
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

#   for testing
#
#def main(args):
#    r = RevertRate(date_start='2008-01-01 00:00:00', date_end='2008-05-01 00:00:00')
#    r.process('156171')
#
## Call Main
#if __name__ == "__main__":
#    sys.exit(main([]))