
__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

# Import python base modules
import data_loader as dl
from user_metrics.config import logging


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
        Base class for providing MySQL table access.  Inherits DataLoader.
        This class is abstract (not enforced) and implements the Template
        Method design pattern.
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
        """ Try to update a record() from the table. """
        return

    def build_table_query(self,
                          select_fields,
                          table_name,
                          where_fields=None,
                          where_ops=None,
                          group_fields=None,
                          order_fields=None):
        """
            Constructs a SQL query given the parameters.

            Parmeters
            ~~~~~~~~~
                select_fields : List(string)
                    Column names to return in query
                where_fields : List(string)
                    Statements which to condition results
                where_ops : List(string)
                    Logical operators on which to combine where statements
                        *[optional]*
                group_fields : List(string)
                    Column names to group on *[optional]*
                order_fields : List(string).
                    Column names to order by *[optional]*

            Return a formatted SQL query constructed from parameters. Note
            that this may be an invalid query if the input was not well formed.
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
                    where_str = where_fields[index] + ' ' + \
                                where_ops[index] + ' '
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

            sql = '%s from %s %s %s %s' % (select_str, table_name, where_str,
                                           group_str, order_str)

        except Exception:
            logging.info('Could not build query for %s: ' % table_name)
            sql = ''

        return sql
