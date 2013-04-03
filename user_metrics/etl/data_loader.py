"""

This module contains the class definitions for datasource access.  The classes
are stateful; they can be modified via class methods that enable data to be
retrieved from the datasource.  For example, the following excerpt from
*DataLoader.execute_SQL()* provides a sample instance of the *_results_* member
being set: ::

    self._cur_.execute(SQL_statement)
    self._db_.commit()

    self._valid_ = True

    self._results_ =  self._cur_.fetchall()
    return self._results_

Additional Class methods implement data processing on retrieved data.  The
*DataLoader.dump_to_csv()* method provides an example of this where the
state of *_results_* is written to a csv file: ::

    output_file = open(projSet.__data_file_dir__ + 'out.tsv', 'wb')

    # Write Column headers
    for index in range(len(column_names)):
        if index < (len(column_names) - 1):
            output_file.write(column_names[index] + '\\t')
        else:
            output_file.write(column_names[index] + '\\n')

    # Write Rows
    for row in self._results_:
        for index in range(len(column_names)):
            if index < (len(column_names) - 1):
                output_file.write(str(row[index]) + '\\t')
            else:
                output_file.write(str(row[index]) + '\\n')

    output_file.close()

The class family structure consists of a base class, DataLoader, which
outlines the basic members and functionality.  This interface is extended
for interaction with specific data sources via inherited classes.

These classes are used to define the data source for the DataReporting family
of classes using an Adapter structural design pattern.

"""

__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"


from time import sleep
import MySQLdb
import operator
import user_metrics.config.settings as projSet

from user_metrics.config import logging


def read_file(file_path_name):
    """ reads a text file line by line """
    with open(file_path_name) as f: content = f.readlines()

    # strip any leading/trailing whitespace
    content = map(lambda s: s.strip(), content)
    return " ".join(content)


class DataLoaderError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Could not perform data operation."):
        Exception.__init__(self, message)


class ConnectorError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Could not establish a connection."):
        Exception.__init__(self, message)


class Connector(object):
    """ This class implements the connection logic to MySQL """

    def __del__(self):
        self.close_db()

    def __init__(self, **kwargs):
        self.set_connection(**kwargs)

    def set_connection(self, retries=20, timeout=1, **kwargs):
        """
            Establishes a database connection.

            Parameters (\*\*kwargs):
                - **db**: string value used to determine the database
                    connection
        """
        if 'instance' in kwargs:
            mysql_kwargs = {}
            for key in projSet.connections[kwargs['instance']]:
                mysql_kwargs[key] = projSet.connections[kwargs['instance']][
                                    key]

            while retries:
                try:
                    self._db_ = MySQLdb.connect(**mysql_kwargs)
                    break
                except MySQLdb.OperationalError as e:
                    logging.debug(__name__ + ' :: Connection dropped. '
                                             'Reopening MySQL connection. '
                                             '{0} retries left, timeout = {1}: '
                                             '"{2}"'.format(retries, timeout,
                                                            e.message))
                    sleep(timeout)
                    retries -= 1
            if not retries:
                raise ConnectorError()

            self._cur_ = self._db_.cursor()

    def close_db(self):
        """ Close the conection if it remains open """
        if hasattr(self, '_cur_'):
            try:
                self._cur_.close()
            except MySQLdb.ProgrammingError:
                pass
        if hasattr(self, '_db_'):
            try:
                self._db_.close()
            except MySQLdb.ProgrammingError:
                pass

    def get_column_names(self):
        """
            Return the column names from the connection cursor (latest
            executed query)

            Return:
                - List(string).  Column names from latest query results.
        """
        try:
            column_data = self._cur_.description
        except AttributeError:
            column_data = []
            logging.error(__name__ + ' :: No column description for this '
                                     'connection.')
        return [elem[0] for elem in column_data]


class DataLoader(object):
    """ Singleton class for performing operations on data sets.
        ETL class for xsv and RDBMS data sources. """

    AND = 'and'
    OR = 'or'

    __instance = None   # Singleton instance

    def __init__(self, **kwargs):
        """ Constructor - Initialize class members and initialize
            the database connection  """
        self.__class__.__instance = self

    def __new__(cls, *args, **kwargs):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(DataLoader, cls).__new__(cls, *args,
                **kwargs)
        return cls.__instance

    def sort_results(self, results, key):
        """
            Takes raw results from a cursor object and sorts them based on a
            tuple unsigned integer key value.

            Parameters:
                - **results**: tuple or list of rows
                - **key**: integer key on which to sort

            Return:
                - List(tuple).  Sorted query results.
        """
        return sorted(results, key=operator.itemgetter(key), reverse=False)

    def cast_elems_to_string(self, input):
        """
            Casts the elements of a list or dictionary structure as strings.

            Parameters:
              - **input**: list or dictionary structure

            Return:
                - List(String), Dict(String), or Boolean.  Structure with
                    string casted elements or boolean=False if the input was
                    malformed.
        """
        if hasattr(input, '__iter__') and hasattr(input, 'keys'):
            output = {}
            for elem in input.keys():
                output[elem] = str(input[elem])
        elif hasattr(input, '__iter__'):
            output = [str(elem) for elem in input]
        else:
            output = str(input)
        return output

    def format_clause(self, elems, index, clause_type, field_name):
        """
            Helper method.  Builds a "WHERE" clause for a SQL statement

            Parameters:
                - **elems** - List(tuple).  Values to be matched in the clause
                - **index** - Integer.  Index of the element value
                - **clause_type** - String.  The logical operator to apply to
                    all statements in the clause
                - **field_name** - String. The name of the field to match in
                    the SQL statement

            Return:
                - String.  "Where" clause.
        """

        clause = ''

        if clause_type == self.AND:
            clause_op = self.AND
        elif clause_type == self.OR:
            clause_op = self.OR
        else:
            clause_op = self.AND

        for row in elems:

            if isinstance(row[index], str):
                value = ''.join(['"',row[index],'"'])
            else:
                value = str(row[index])

            clause = "".join([clause,
                              '%(field_name)s = %(value)s %(clause_op)s ' %
                              {'field_name' : field_name,
                               'clause_op' : clause_op,
                               'value' : value}])
        clause = clause[:-4]

        return clause

    def format_comma_separated_list(self, elems, include_quotes=True):
        """
            Produce a comma separated list from a list of elements.

            Parameters:
                - **elems** - List.  Elements to format as csv string
                - **include_quotes** - Boolean.  Determines whether the
                    return string inserts quotes around the elements

            Return:
                - String.  Formatted comma separated string of the list
                    elements
        """

        if include_quotes:
            elems = map(lambda x: MySQLdb.escape_string(x), elems)
            join_tag = '" <join_tag_1234> "'
        else:
            join_tag = ' <join_tag_1234> '

        elems_str = join_tag.join(elems)
        elems_str = ",".join(elems_str.split(join_tag[1:-1]))

        if include_quotes:
            elems_str = elems_str.join(['"','"'])

        return elems_str

    def get_elem_from_nested_list(self, in_list, index):
        """
            Parse element from separated value file.  Return a list
            containing the values matched on each line of the file.

            Usage: ::

                >>> el = DL.ExperimentsLoader()
                >>> results = el.execute_SQL(SQL_query_string)
                >>> new_results = el.get_elem_from_nested_list(results,0)

            Parameters:
                - **in_list**: List(List(\*)). List of lists from which
                    to parse elements.
                - **index**: Integer. Index of the element to retrieve

            Return:
                - List(\*).  List of sub-elements parsed from list.
        """

        out_list = list()

        for elem in in_list:
            try:
                out_list.append(elem[index])
            except Exception:
                logging.info('Unable to extract index %s from %s' % (
                    str(index), str(elem)))

        return out_list

    def list_from_xsv(self, xsv_name, separator='\t', header=False):
        """
            Parse element from separated value file.  Return a list
            containing the values matched on each line of the file.

            Parameters:
                - **xsv_name**: String.  filename of the .xsv; it is
                    assumed to live in the project data folder
                - **index**: Integer. Index of the element to retrieve
                - **separator**: String.  The separating character in
                    the file.  Default to tab.
                - **header**: Boolean.  Flag indicating whether the
                    file has a header.

            Return:
                - List(string).  List of elements parsed from xsv.
        """
        out = list()
        try:
            xsv_file = open(projSet.__data_file_dir__ + xsv_name, 'r')
        except IOError as e:
            logging.info('Could not open xsv for writing: %s' % e.message)
            return out

        # Process file line-by-line
        if header: xsv_file.readline()
        while 1:
            line = xsv_file.readline().strip()
            if line == '': break
            tokens = line.split(separator)
            out.append([str(tokens[index]) for index in xrange(len(tokens))])
        return out

    def list_to_xsv(self, nested_list, separator='\t', log=False,
                    outfile='list_to_xsv.out'):
        """
            Transforms a nested list or t

            Parameters:
                - **nested_list** - List(List()). Nested list to insert to xsv.
                - **separator**: String.  The separating character in the file.
                    Default to tab.
        """
        try:
            file_obj = open(projSet.__data_file_dir__ + outfile, 'w')
        except IOError as e:
            logging.error(__name__ + ' :: Could not open '
                                     'xsv for writing: %s' % e.message)
            return

        if hasattr(nested_list, '__iter__'):
            for elem in nested_list:
                new_elems = self.cast_elems_to_string(elem)
                line_in = separator.join(new_elems) + '\n'
                try:
                    file_obj.write(line_in)
                except IOError:
                    if log: logging.error('Could not write: "%s"' %
                                          str(line_in.strip()))
        else:
            logging.error('Expected an iterable to write to file.')

        file_obj.close()


    def remove_duplicates(self, l):
        """
            Removes duplicates from a separated value file and write the
            de-duped results to a new file.  The output file overwrites
            the input file unless a new extension is specified.

            Parameters:
                - **l** - String.  The .xsv filename, assumed to be located
                    in the project data folder.
                - **index** - list(int).  Indices of fields to compare.
                    Default is all.

            Return:
                - empty.
        """
        # Rather than a list use a hash to store each line
        duplicates = dict()
        new_list= list()

        for e in l:
            str_e = e.__str__()
            if not duplicates.has_key(str_e):
                duplicates[str_e] = 0
                new_list.append(e)
            else:
                duplicates[str(e)] += 1
        return new_list


    def write_dict_to_xsv(self, d, separator="\t", outfile='dict_to_xsv.out'):
        """
            Write the contents of a dictionary whose values are lists to a file

            Parameters:
                - **d** - dict(list).  The dictionary to write from.
                - **outfile** - String.  The output filename, assumed to be
                    located in the project data folder.
                - **separator** - String.  The separating character in the
                    output file.  Default to tab.

            Return:
                - empty.
        """

        file_obj_out = open(projSet.__data_file_dir__ + outfile, 'w')

        # Determine the length of each key-list and store
        max_lens = dict()
        max_list_len = 0
        for key in d.keys():
            try:
                max_lens[key] = len(d[key])
            except TypeError:
                raise DataLoaderError(__name__ +
                                      ' :: dict must contain lists.')
            if max_lens[key] > max_list_len:
                max_list_len = max_lens[key]

        # Write to xsv
        file_obj_out.write(separator.join(d.keys()) + '\n')
        for i in range(max_list_len):
            line_elems = list()
            for key in d:
                if i < max_lens[key]:
                    try:
                        line_elems.append(str(d[key][i]))
                    except IndexError:
                        raise DataLoaderError(
                            __name__ + ' :: dict must contain lists.')
                else:
                    line_elems.append('None')
            file_obj_out.write(separator.join(line_elems) + '\n')

        file_obj_out.close()


    def format_condition_in(self, field_name, item_list, include_quotes=False):
        """ Formats a SQL "in" condition """
        if hasattr(item_list, '__iter__'):
            list_str = self.format_comma_separated_list(
                self.cast_elems_to_string(item_list),
                include_quotes=include_quotes)
            list_cond = "%s in (%s)" % (field_name, list_str)
            return list_cond
        else:
            logging.error(__name__ + '::format_condition_in - '
                                     'item_list must implement the '
                                     'iterable interface.')
            return ''


    def format_condition_between(self, field_name, val_1, val_2,
                                 include_quotes=False):
        """ Formats a SQL "between" condition """

        # Cast operands to string.  Add quoatation if specified
        val_1 = str(val_1)
        val_2 = str(val_2)
        if include_quotes:
            val_1 = '"' + val_1 + '"'
            val_2 = '"' + val_2 + '"'

        # Format clause
        ts_cond = '%(field_name)s BETWEEN %(val_1)s AND %(val_2)s' % {
            'field_name' : field_name,
            'val_1' : val_1,
            'val_2' : val_2,
            }
        return ts_cond

