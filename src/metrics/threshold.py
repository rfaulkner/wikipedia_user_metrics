
__author__ = "Ryan Faulkner"
__date__ = "December 6th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import user_metric as um

class Threshold(um.UserMetric):
    """
        Boolean measure: Did an editor reach some threshold of activity (e.g. n edits, words added, pages created,
        etc.) within t time.

            `https://meta.wikimedia.org/wiki/Research:Metrics/threshold(t,n)`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * boolean flag to indicate whether edit threshold was reached in time given

        usage e.g.: ::

            >>> import src.etl.threshold as t
            >>> for r in t.Threshold().process([13234584]).__iter__(): print r
            (13234584L, 1)
    """

    def __init__(self,
                 date_start='2001-01-01 00:00:00',
                 date_end=datetime.datetime.now(),
                 t=1440,
                 n=1,
                 **kwargs):

        """
            - Parameters:
                - **date_start**: string or datetime.datetime. start date of edit interval
                - **date_end**: string or datetime.datetime. end date of edit interval
        """
        self._start_ts_ = self._get_timestamp(date_start)
        self._end_ts_ = self._get_timestamp(date_end)
        self._t_ = t
        self._n_ = n

        um.UserMetric.__init__(self, **kwargs)

    @staticmethod
    def header():
        return ['user_id', 'has_reached_threshold']

    def process(self, user_handle, is_id=True, **kwargs):
        """
            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids
        """
        # Format condition on user ids.  if no user handle exists there is no condition.
        if user_handle:
            if not hasattr(user_handle, '__iter__'): user_handle = [user_handle]
            user_id_str = um.dl.DataLoader().format_comma_separated_list(
                um.dl.DataLoader().cast_elems_to_string(user_handle), include_quotes=False)
            user_id_cond = "and user_id in (%s)" % user_id_str
        else:
            user_id_cond = ''

        # get all registrations in the time period
        sql = """
            select
                user_id,
                user_registration
            from %(project)s.user
            where user.user_registration >= %(date_start)s and user.user_registration <= %(date_end)s
                %(uid_str)s
        """ % {
            'project' : self._project_,
            'date_start' : self._start_ts_,
            'date_end' : self._end_ts_,
            'uid_str' : user_id_cond
        }

        self._data_source_._cur_.execute(" ".join(sql.strip().split('\n')))

        # for each user get

        sql = """
            select
                count(*) as revs
            from %(project)s.revision as r
                join %(project)s.page as p
                on r.rev_page = p.page_id
            where p.page_namespace = %(ns)s and rev_timestamp <= %(ts)s and rev_id = %(id)s
        """
        sql = " ".join(sql.strip().split('\n'))

        self._results = list()
        for r in self._data_source_._cur_:
            try:
                ts = self._get_timestamp(um.date_parse(r[1]) + datetime.timedelta(minutes=self._t_))
                id = long(r[0])

                self._data_source_._cur_.execute(sql % {'project' : self._project_, 'ts' : ts,
                                                        'ns' : self._namespace_, 'id' : id})
                count = int(self._data_source_._cur_.fetchone()[0])
            except IndexError: continue
            except ValueError: continue

            if count < self._n_:
                self._results.append((r[0],0))
            else:
                self._results.append((r[0],1))
        return self

# testing
if __name__ == "__main__":
    for r in Threshold().process([13234584]).__iter__(): print r