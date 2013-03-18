
__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

# Import python base modules
import sys
import logging
import data_loader as dl

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def decorator_virtual_func_table_loader(f):
    """ This decorator is used to render certain functions virtual """
    def wrapper(self):
        if self.__class__ == TableLoader:
            return 'TableLoader is virtual.  Use subclass.'
        else:
            return f(self)
    return wrapper

class TableLoader(dl.Connector):
    """
        Base class for providing MySQL table access.  Inherits DataLoader.  This class is abstract (not enforced) and implements the
        Template Method design pattern.
    """

    def __init__(self, **kwargs):
        """ Initialize parent class. """
        super(TableLoader, self).__init__(**kwargs)
        self._table_name_ = 'meta'
        # self.set_connection(**kwargs)

    @decorator_virtual_func_table_loader
    def record_exists(self, **kwargs):
        """ Returns a boolean value reflecting whether a record exists in the table. """
        return

    @decorator_virtual_func_table_loader
    def insert_row(self, record_list, **kwargs):
        """ Try to insert a new record (s)into the table. """
        return

    @decorator_virtual_func_table_loader
    def delete_row(self, **kwargs):
        """ Try to delete a record() from the table. """
        return

    @decorator_virtual_func_table_loader
    def update_row(self, set_col, set_vals, id_field, ids):
        """
            Issues generic update quer(ies).  Try to modify a record(s) in the table.

            An example of how this method may be seen when updating a user_hash field in db42.pmtpa.wmnet.rfaulk.e3_pef_iter1_log_data
            (taken from work on the Post-Edit Feedback E3 experiment - https://meta.wikimedia.org/wiki/Research:Edit_feedback): ::

                >>> import classes.DataLoader as DL
                >>> dl = DL.e3_pef_iter1_log_data_Loader()
                >>> el = DL.ExperimentsLoader()
                >>> results = dl.execute_SQL('select rev_id, rev_user from rfaulk.e3_pef_iter1_log_data join enwiki.revision on rev_id = e3pef_rev_id where e3pef_event regexp "1-postEdit"')
                >>> rev_ids = el.get_elem_from_nested_list(results, 0)
                >>> user_ids = el.get_elem_from_nested_list(results, 1)
                >>> dl.update_row('e3pef_user_hash', user_ids, 'e3pef_rev_id', rev_ids)
                Aug-06 15:31:09 INFO     32677 rows successfully updated in rfaulk.e3_pef_iter1_log_data

            First the revision and corresponding user ids need to be retrieved from the revision table.  These results are then used to update
            the records in e3_pef_iter1_log_data.

                - Parameters:
                    - **set_col**: String. Name of column to update
                    - **set_vals**: List(\*). update values
                    - **id_field**: String. Name of column on which to ID record(s).
                    - **ids**: List(\*).

                - Return.
                    - empty.
        """

        sql = 'update %(tablename)s set %(set_col)s = %(set_val)s where %(id_field)s = %(id)s'
        rows_updated = 0

        try:
            for i in range(len(set_vals)):

                set_val = str(set_vals[i])
                id = str(ids[i])

                self.execute_SQL(sql % {'tablename' : self._table_name_, 'set_col' : set_col, 'id_field' : id_field, 'set_val' : set_val, 'id' : id})
                rows_updated += 1
        except Exception:
            logging.info('Failed to assign all values.')

        logging.info('%s rows successfully updated in %s' % (str(rows_updated), self._table_name_))

    def build_table_query(self, select_fields, table_name, where_fields=None, where_ops=None, group_fields=None, order_fields=None):
        """
            Constructs a SQL query given the parameters.

            - Parmeters:
                - **select_fields**: List(string). Column names to return in query
                - **where_fields**: List(string). Statements which to condition results
                - **where_ops**: List(string). Logical operators on which to combine where statements *[optional]*
                - **group_fields** List(string). Column names to group on *[optional]*
                - **order_fields**: List(string). Column names to order by *[optional]*

            - Return
                - String.  Formatted SQL query constructed from parameters.  Note that this may be an invalid query if the input was not well formed.
        """

        # Pre- process defaults
        if where_fields is None: where_fields = []
        if where_ops is None: where_ops = []
        if group_fields is None: group_fields = []
        if order_fields is None: order_fields = []

        # Begin function
        try:

            select_str = 'select '
            for field in select_fields:
                select_str = field + ','
            select_str = select_str[:-1]

            if where_fields:
                where_str = 'where '
                for index in range(len(where_ops)):
                    where_str = where_fields[index] + ' ' + where_ops[index] + ' '
                where_str = where_str + where_fields[len(where_ops)]
            else:
                where_str = ''

            if group_fields:
                group_str = 'group by '
                for field in group_fields:
                    group_str = field + ','
                group_str = group_str[:-1]
            else:
                group_str = ''

            if order_fields:
                order_str = 'order by '
                for field in order_fields:
                    order_str = field + ','
                order_str = order_str[:-1]
            else:
                order_str = ''

            sql = '%s from %s %s %s %s' % (select_str, table_name, where_str, group_str, order_str)

        except Exception:
            logging.info('Could not build query for %s: ' % table_name)
            sql = ''

        return sql
