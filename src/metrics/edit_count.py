
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import datetime
import user_metric as um

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

    # Structure that defines parameters for EditRate class
    _param_types = {
        'init' : {
            'date_start' : ['str|datetime', 'Earliest date a block is measured.','2010-01-01 00:00:00'],
            'date_end' : ['str|datetime', 'Latest date a block is measured.',datetime.datetime.now()],
            },
        'process' : {
            'is_id' : ['bool', 'Are user ids or names being passed.',True],
            }
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields' : [0],
        'date_fields' : [],
        'float_fields' : [],
        'integer_fields' : [1],
        'boolean_fields' : [],
        }

    # define class indices for aggregators
    EditCount.class_preprocessing()

    @um.pre_metrics_init
    def __init__(self, **kwargs):

        um.UserMetric.__init__(self, **kwargs)

        self._start_ts_ = self._get_timestamp(kwargs['date_start'])
        self._end_ts_ = self._get_timestamp(kwargs['date_end'])

    @staticmethod
    def header(): return ['user_id', 'edit_count']

    def process(self, user_handle, **kwargs):
        """
            Determine edit count.  The parameter *user_handle* can be either a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and as a user_name for string input.  If a list of users is passed
            to the *process* method then a dict object with edit counts keyed by user handles is returned.

            - Paramters:
                - **user_handle** - String or Integer (optionally lists):  Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle stores user names or user ids
        """

        self.apply_default_kwargs(kwargs,'process')
        is_id=kwargs['is_id']

        edit_count = list()
        ts_condition  = 'and rev_timestamp >= "%s" and rev_timestamp < "%s"' % (self._start_ts_, self._end_ts_)

        # Escape user_handle for SQL injection
        user_handle = self._escape_var(user_handle)

        # determine the format field
        field_name = ['rev_user_text','rev_user'][is_id]

        if not hasattr(user_handle, '__iter__'): user_handle = [user_handle] # ensure the handles are iterable
        user_set = um.dl.DataLoader().format_comma_separated_list(user_handle)
        sql = """
                select
                    %(field_name)s,
                    count(*)
                from %(project)s.revision
                where %(field_name)s in (%(user_set)s) %(ts_condition)s
                group by 1
                """ % {
            'field_name' : field_name,
            'user_set' : user_set,
            'ts_condition' : ts_condition,
            'project' : self._project_}
        sql = " ".join(sql.strip().split())

        try:
            self._data_source_._cur_.execute(sql)
        except um.MySQLdb.ProgrammingError:
            raise um.UserMetric.UserMetricError(message=str(self.__class__()) + 'Could not get edit counts - Query failed.')

        for row in self._data_source_._cur_.fetchall(): edit_count.append([row[0], int(row[1])])
        self._results = edit_count
        return self