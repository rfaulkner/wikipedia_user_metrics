"""

This module contains the class definitions for datasource access.  The classes are stateful; they can be modified via class methods
that enable data to be retrieved from the datasource.  For example, the following excerpt from *DataLoader.execute_SQL()* provides a sample
instance of the *_results_* member being set: ::

    self._cur_.execute(SQL_statement)
    self._db_.commit()

    self._valid_ = True

    self._results_ =  self._cur_.fetchall()
    return self._results_

Additional Class methods implement data processing on retrieved data.  The *DataLoader.dump_to_csv()* method provides an example of this
where the state of *_results_* is written to a csv file: ::

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

The class family structure consists of a base class, DataLoader, which outlines the basic members and functionality.  This interface is extended
for interaction with specific data sources via inherited classes.

These classes are used to define the data source for the DataReporting family of classes using an Adapter structural design pattern.

"""

__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import MySQLdb
import datetime
import cgi
from urlparse import urlparse
import re
import logging
import gzip
import operator
from dateutil.parser import parse as date_parse
import config.settings as projSet
import timestamp_processor as tp

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def read_file(file_path_name):
    """ reads a text file line by line """
    with open(file_path_name) as f:
        content = f.readlines()
    f.close()
    content = map(lambda s: s.strip(), content) # strip any leading/trailing whitespace
    return " ".join(content)


class Connector(object):
    """ This class implements the connection logic to MySQL """

    def __del__(self):
        self.close_db()

    def __init__(self, **kwargs):
        self.set_connection(**kwargs)

    def set_connection(self, **kwargs):
        """
            Establishes a database connection.

            Parameters (\*\*kwargs):
                - **db**: string value used to determine the database connection
        """
        if 'instance' in kwargs:
            logging.info(self.__str__() + '::Connecting data loader to "%s"' % kwargs['instance'])

            mysql_kwargs = {}
            for key in projSet.connections[kwargs['instance']]:
                mysql_kwargs[key] = projSet.connections[kwargs['instance']][key]

            self.close_db()
            self._db_ = MySQLdb.connect(**mysql_kwargs)
            self._cur_ = self._db_.cursor()

    def close_db(self):
        if hasattr(self, '_cur_'):
            self._cur_.close()
        if hasattr(self, '_db_'):
            self._db_.close()

    def get_column_names(self):
        """
            Return the column names from the connection cursor (latest executed query)

            Return:
                - List(string).  Column names from latest query results.
        """
        try:
            column_data = self._cur_.description
        except AttributeError:
            column_data = []
            logging.error(__name__ + ':: No column description for this connection.')
        return [elem[0] for elem in column_data]

    def execute_SQL(self, SQL_statement):
        """
            Executes a SQL statement and return the raw results.

            Parameters:
                - **SQL_statement**: String. variable storing the SQL query

            Return:
                - List(tuple).  The query results (or -1 for a failed query).
        """

        try:
            self._cur_.execute(SQL_statement)
            self._db_.commit()

        except MySQLdb.ProgrammingError as inst:
            self._db_.rollback()
            logging.error(inst.__str__())       # __str__ allows args to printed directly

        return self._cur_.fetchall()

class DataLoader(object):
    """
        Base class for loading data from a specified source.  This is a Singleton class.

        The general functionality is as follows:

            * Initializes remote database connections
            * Provides entry-point for execution of generic queries
            * Functionality for processing results from query execution
            * Functionality for outputting results from query execution

        Class members:

            - **_results_**: stores the output from the latest SQL execution
            - **_col_names_**: column names from the latest SQL execution
            - **_valid_**: flag that indicates whether the current results are valid
    """

    AND = 'and'
    OR = 'or'
    __instance = None

    def __init__(self, **kwargs):
        """ Constructor - Initialize class members and initialize the database connection  """
        self.__class__.__instance = self
        self._conn_ = Connector(**kwargs)

    def __new__(cls, *args, **kwargs):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(DataLoader, cls).__new__(cls, *args, **kwargs)
        else:
            try:
                cls.__instance.connection=kwargs['instance']
            except KeyError:
                pass
        return cls.__instance

    @property
    def connection(self):
        return self._conn_

    @connection.setter
    def connection(self, value):
        self._conn_.set_connection(**{'instance' : value})
    @connection.getter
    def connection(self):
        return self._conn_

    def sort_results(self, results, key):
        """
            Takes raw results from a cursor object and sorts them based on a tuple unsigned integer key value.

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
                - List(String), Dict(String), or Boolean.  Structure with string casted elements or boolean=False if the input was malformed.
        """
        if hasattr(input, '__iter__') and hasattr(input, 'keys'):
            output = [str(elem) for elem in input]
        elif hasattr(input, '__iter__'):
            output = dict()
            for elem in input.keys():
                output[elem] = str(input[elem])
        else:
            return False

        return output

    def dump_to_csv(self, results, column_names):
        """
            Data Processing - take **__results__** and dump into out.tsv in the data directory

            Parameters (\*\*kwargs):
                - **column_names** - list of strings storing the column names

            Return:
                - empty.
        """

        logging.info('Writing results to: ' + projSet.__data_file_dir__ + 'out.tsv')
        output_file = open(projSet.__data_file_dir__ + 'out.tsv', 'wb')

        # Write Column headers
        for index in range(len(column_names)):
            if index < (len(column_names) - 1):
                output_file.write(column_names[index] + '\t')
            else:
                output_file.write(column_names[index] + '\n')

        # Write Rows
        for row in results:
            for index in range(len(column_names)):
                if index < (len(column_names) - 1):
                    output_file.write(str(row[index]) + '\t')
                else:
                    output_file.write(str(row[index]) + '\n')

        output_file.close()


    def format_clause(self, elems, index, clause_type, field_name):
        """
            Helper method.  Builds a "WHERE" clause for a SQL statement

            Parameters:
                - **elems** - List(tuple).  Values to be matched in the clause
                - **index** - Integer.  Index of the element value
                - **clause_type** - String.  The logical operator to apply to all statements in the clause
                - **field_name** - String. The name of the field to match in the SQL statement

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

            clause = "".join([clause, '%(field_name)s = %(value)s %(clause_op)s ' % {'field_name' : field_name, 'clause_op' : clause_op, 'value' : value}])
        clause = clause[:-4]

        return clause

    def format_comma_separated_list(self, elems, include_quotes=True):
        """
            Produce a comma separated list from a list of elements.

            Parameters:
                - **elems** - List.  Elements to format as csv string
                - **include_quotes** - Boolean.  Determines whether the return string inserts quotes around the elements

            Return:
                - String.  Formatted comma separated string of the list elements
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
            Parse element from separated value file.  Return a list containing the values matched on each line of the file.

            Usage: ::

                >>> el = DL.ExperimentsLoader()
                >>> results = el.execute_SQL(SQL_query_string)
                >>> new_results = el.get_elem_from_nested_list(results,0)

            Parameters:
                - **in_list**: List(List(\*)). List of lists from which to parse elements.
                - **index**: Integer. Index of the element to retrieve

            Return:
                - List(\*).  List of sub-elements parsed from list.
        """

        out_list = list()

        for elem in in_list:
            try:
                out_list.append(elem[index])
            except Exception:
                logging.info('Unable to extract index %s from %s' % (str(index), str(elem)))

        return out_list

    def get_elem_from_xsv(self, xsv_name, index, separator='\t', header=True):
        """
            Parse element from separated value file.  Return a list containing the values matched on each line of the file.

            Parameters:
                - **xsv_name**: String.  filename of the .xsv; it is assumed to live in the project data folder
                - **index**: Integer. Index of the element to retrieve
                - **separator**: String.  The separating character in the file.  Default to tab.
                - **header**: Boolean.  Flag indicating whether the file has a header.

            Return:
                - List(string).  List of elements parsed from xsv.
        """

        elems = list()
        xsv_file = open(projSet.__data_file_dir__ + xsv_name, 'r')

        if header:
            xsv_file.readline()

        line = xsv_file.readline()
        while line != '':
            tokens = line.split(separator)

            # If index is the last token remove the newline character
            if index + 1 == len(tokens):
                elems.append(str(tokens[index]).strip())
            else:
                elems.append(str(tokens[index]))
            line = xsv_file.readline()

        return elems

    def list_to_xsv(self, nested_list, separator='\t'):
        """
            Transforms a nested list or t

            Parameters:
                - **nested_list** - List(List()).  Nested list to insert to xsv.
                - **separator**: String.  The separating character in the file.  Default to tab.

            Return:
                - empty.
        """

        file_obj = open(projSet.__data_file_dir__ + 'list_to_xsv.out', 'w')

        for elem in nested_list:
            try:
                new_elems = self.cast_elems_to_string(elem)
                line_in = separator.join(new_elems) + '\n'
                file_obj.write(line_in)
            except:
                logging.error('Could not parse: "%s"' % str(elem))

        file_obj.close()

    def create_table_from_xsv(self, filename, create_sql, table_name, parse_function=None,
                              max_records=10000, user_db=projSet.connections['slave']['db'],
                              regex_list=None, neg_regex_list=None, header=True, separator='\t'):
        """
            Populates or creates a table from a .xsv file.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **create_sql** - String.  Contains the SQL create statement for the table (if necessary).
                - **table_name** - String.  Name of table to populate.
                - **parse_function** - Method Pointer.  Method that performs line parsing (see helper class TransformMethod)
                - **create_table** - Boolean.  Flag that indicates whether table creation is to occur.
                - **max_record** - Integer. Maximum number of records allowable in insert statement.
                - **user_db** - String. Database instance.
                - **regex_list** - List(string).  List of inclusive regex strings over which each line of input will be conditioned.
                - **neg_regex_list** - List(string).  List of exclusive regex strings over which each line of input will be conditioned.
                - **header**: Boolean.  Flag indicating whether the file has a header.
                - **separator**: String.  The separating character in the file.  Default to tab.

            Return:
                - empty.
        """

        # Open the data file - Process the header
        if re.search('\.gz', filename):
            file_obj = gzip.open(projSet.__data_file_dir__ + filename, 'rb')
        else:
            file_obj = open(projSet.__data_file_dir__ + filename, 'r')
        if header:
            file_obj.readline()

        # Optionally create the table - if no create sql is specified create a generic tbale based on column names
        if create_sql:
            try:
                self._conn_.execute_SQL("drop table if exists `%s`.`%s`" % (user_db, table_name))
                self._conn_.execute_SQL(create_sql)

            except Exception:
                logging.error('Could not create table: %s' % create_sql)
                return

        # Get column names - reset the values if header has already been set
        self._conn_.execute_SQL('select * from `%s`.`%s` limit 1' % (user_db, table_name))
        column_names = self._conn_.get_column_names()
        column_names_str = self.format_comma_separated_list(column_names, include_quotes=False)

        # Prepare SQL syntax
        sql = """
                        insert into `%(user_db)s`.`%(table_name)s`
                        (%(column_names)s) values
                    """ % {
                'table_name' : table_name,
                'column_names' : column_names_str,
                'user_db' : user_db}

        insert_sql = " ".join(sql.strip().split())

        # Crawl the log line by line - insert the contents of each line into the table
        count = 0
        line = file_obj.readline().strip(separator)
        while line != '':

            # Perform batch insert if max is reached
            if count % max_records == 0 and count:
                logging.info('Inserting %s records. Total = %s' % (str(max_records), str(count)))
                self._conn_.execute_SQL(insert_sql[:-2])
                insert_sql = " ".join(sql.strip().split())

            # Determine whether the row qualifies for insertion based on regex patterns
            include_line = True

            # positive patterns
            if isinstance(regex_list, list):
                for r in regex_list:
                    if not(re.search(r, line)):
                        line = file_obj.readline().strip()
                        include_line = False
                        break

            # negative patterns
            if isinstance(neg_regex_list, list):
                for r in neg_regex_list:
                    if re.search(r, line):
                        line = file_obj.readline().strip()
                        include_line = False
                        break

            if not include_line:
                continue

            # Parse input line
            if not parse_function:
                insert_field_str = self.format_comma_separated_list(line.split(separator))
            else:
                insert_field_str = self.format_comma_separated_list(parse_function(line))

            # Only add the record
            if len(insert_field_str.split(',')) == len(column_names):
                insert_sql += '(%s), ' % insert_field_str
                count += 1

            line = file_obj.readline().strip(separator)

        # Perform insert
        if count:
            logging.info('Inserting remaining records. Total = %s' % str(count))
            self._conn_.execute_SQL(insert_sql[:-2])

    def remove_duplicates_from_xsv(self, filename, separator='\t', index=None, header=True, opt_ext=".dup"):
        """
            Removes duplicates from a separated value file and write the de-duped results to a new file.  The output file
            overwrites the input file unless a new extension is specified.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **separator** - String.  The separating character in the file.  Default to tab.
                - **index** - Integer.  Index of field in each line to evaluate on.
                - **header** - Boolean.  Flag indicating whether the file has a header.
                - **opt_ext** - String.  Determines the optional extension to the output file.

            Return:
                - empty.
        """

        file_obj = open(projSet.__data_file_dir__ + filename, 'r')

        # Rather than a list use a hash to store each line
        lines = dict()

        if header:
            header_str = file_obj.readline()

        line = file_obj.readline()
        while line != '':
            if index == None:
                lines[line] = 0
            else:
                try:
                    elems = line.split(separator)
                    lines[elems[index]] = line
                except:
                    logging.error('Could not parse line: "%s"' % line)

            line = file_obj.readline()

        file_obj.close()

        file_obj = open(projSet.__data_file_dir__ + filename + opt_ext, 'w')

        if header:
            file_obj.write(header_str)

        # Write non-duplicates to the outfile
        # If no index was specified the hash-key is the line itself
        for key in lines.keys():
            if index == None:
                file_obj.write(key)
            else:
                file_obj.write(lines[key])

    def remove_elems_from_xsv(self, filename, elems, index, separator='\t',
                              header=True, inclusive=True, opt_ext='.rem', regex_pattern=None):
        """
            Evaluates each line of an .xsv on a condition.  The field is conditioned on being contained in a list or on matching a regex.  The results
            overwrite the input unless an optional extension is specified.  To choose to condition on matching a list of strings the parameter
            *regex_pattern* must not be set to *None*.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **elems** - List(string).  List of elements on which to condition the field to evaluate.
                - **separator** - String.  The separating character in the file.  Default to tab.
                - **index** - Integer.  Index of field in each line to evaluate on.
                - **header** - Boolean.  Flag indicating whether the file has a header.
                - **inclusive** - Boolean.  Flag determining whether the regex is inclusive or exclusive.
                - **opt_ext** - String.  Determines the optional extension to the output file.
                - **regex_pattern** - List(string).  List of regex patterns on which to match.

            Return:
                - empty.
        """

        file_obj = open(projSet.__data_file_dir__ + filename, 'r')
        use_regex = regex_pattern != None and isinstance(regex_pattern, str)

        # Rather than a list use a hash to store each line
        lines = dict()

        if header:
            header_str = file_obj.readline()

        # Select lines to be included in the output
        line = file_obj.readline()
        while line != '':
            try:

                tokens = line.split(separator)
                if inclusive:
                    if use_regex:
                        if re.search(regex_pattern, tokens[index]):
                            lines[tokens[index]] = line
                    else:
                        if tokens[index] in elems:
                            lines[tokens[index]] = line
                else:
                    if use_regex:
                        if not(re.search(regex_pattern, tokens[index])):
                            lines[tokens[index]] = line
                    else:
                        if not(tokens[index] in elems):
                            lines[tokens[index]] = line

            except:
                logging.error('Could not parse line: "%s"' % line)

            line = file_obj.readline()

        file_obj.close()

        file_obj = open(projSet.__data_file_dir__ + filename + opt_ext, 'w')

        if header:
            file_obj.write(header_str)

        # Write to the outfile
        # If no index was specified the hash-key is the line itself
        for key in lines.keys():
            file_obj.write(lines[key])

    def extract_pattern_from_text_file(self, filename, parse_method, header=True):
        """
            Extracts selected elements from a text file on a line by line basis by using a parsing method on each line.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **parse_method** - Method Pointer.  Method that handles extracting content from each line (see helper class XSVParseMethods).
                - **header** - Boolean.  Flag indicating whether the file has a header.

            Return:
                - List.  List of elements parsed from each line of the input.
        """

        elements = list()
        file_obj = open(projSet.__data_file_dir__ + filename, 'r')

        if header:
            file_obj.write(header_str)

        line = file_obj.readline()
        while line != '':
            # logging.debug(line)

            try:
                element = parse_method(line)
                # print line
            except:
                line = file_obj.readline()
                continue

            if element != '':
                elements.append(element)

            line = file_obj.readline()

        file_obj.close()

        return elements

    def transform_xsv(self, filename, index_generator_methods=None, separator_from='\t',
                      separator_to='\t', outfile = None, header=False, **kwargs):
        """
            Transform the fields of an xsv file using transform method pointers.  The outfile by default is named as the input file with the extension
            '.trn' appended.  The field separator may also optionally be changed.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **index_generator_methods** - List(Method Pointer).  Methods used to transform input (see helper class TransformMethods).
                - **separator_from** - String.  The separating character in the in file.  Default to tab.
                - **separator_to** - String.  The separating character in the out file.  Default to tab.
                - **outfile** - String.  Filename that allows overriding of the default outfile name.
                - **header** - Boolean.  Flag indicating whether the file has a header.

            Return:
                - empty.
        """

        # Pre- process defaults
        if index_generator_methods is None: index_generator_methods = []

        # Begin function
        if outfile == None:
            file_obj_out = open(projSet.__data_file_dir__ + filename + '.trn', 'w')
        else:
            file_obj_out = open(projSet.__data_file_dir__ + outfile, 'w')

        file_obj_in = open(projSet.__data_file_dir__ + filename, 'r')

        if header:
            file_obj_in.readline()

        line = file_obj_in.readline()
        # Read each line of the xsv
        while line != '':

            fields = list()
            tokens = line.split(separator_from)

            # apply the index generator
            for index in range(len(index_generator_methods)):
                fields.append(index_generator_methods[index](tokens, index, **kwargs))

            file_obj_out.write(separator_to.join(fields) + '\n')
            line = file_obj_in.readline()

        file_obj_in.close()
        file_obj_out.close()

    def create_xsv_from_SQL(self, sql, outfile = 'sql_to_xsv.out', separator = '\t'):
        """
            Generate an xsv file from SQL output.  The rows from the query resutls are written to a file using the specified field separator.

            Parameters:
                - **sql** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **outfile** - String.  The output filename, assumed to be located in the project data folder.
                - **separator** - String.  The separating character in the output file.  Default to tab.

            Return:
                - List.  List of elements parsed from each line of the input.
        """

        file_obj_out = open(projSet.__data_file_dir__ + outfile, 'w')
        results = self._conn_.execute_SQL(sql)

        for row in results:
            line_str = ''
            for elem in row:
                line_str = line_str + str(elem).strip() + separator
            line_str = line_str[:-1] + '\n'
            file_obj_out.write(line_str)
        file_obj_out.close()

    def write_dict_to_xsv(self, d, separator="\t", outfile='dict_to_xsv.out'):
        """
            Write the contents of a dictionary whose values are lists to a file

            Parameters:
                - **d** - dict(list).  The dictionary to write from.
                - **outfile** - String.  The output filename, assumed to be located in the project data folder.
                - **separator** - String.  The separating character in the output file.  Default to tab.

            Return:
                - empty.
        """

        file_obj_out = open(projSet.__data_file_dir__ + outfile, 'w')

        # All keys must reference a list
        for key in d.keys():
            if not(isinstance(d[key],list)):
                logging.error('DataLoader::write_dict_to_xsv - All keys must index lists, bad key = %s' % key)
                raise Exception('DataLoader::write_dict_to_xsv - All keys must index lists, bad key = %s' % key)

        # Determine the length of each key-list and store
        max_lens = dict()
        max_list_len = 0
        for key in d.keys():
            max_lens[key] = len(d[key])
            if max_lens[key] > max_list_len:
                max_list_len = max_lens[key]

        # Write to xsv
        file_obj_out.write(separator.join(d.keys()) + '\n')
        for i in range(max_list_len):
            line_elems = list()
            for key in d:
                if i < max_lens[key]:
                    line_elems.append(str(d[key][i]))
                else:
                    line_elems.append('None')
            file_obj_out.write(separator.join(line_elems) + '\n')

        file_obj_out.close()

    def create_generic_table(self, table_name, column_names):
        """
            Given a table name and a set of column names create a generic table

            Parameters:
                - **table_name** - str.
                = **column_names** - list(str).
        """
        create_stmt = 'CREATE TABLE `%s` (' % table_name
        for col in column_names:
            create_stmt += "`%s` varbinary(255) NOT NULL DEFAULT ''," % col
        create_stmt = create_stmt[:-1] + ") ENGINE=MyISAM DEFAULT CHARSET=binary"
        self._conn_.execute_SQL(create_stmt)

    class TransformMethods():
        """
            Helper Class - Stores transformation methods to act on a set of tokens stored as strings.  Each method in this class takes two arguments:

                - **tokens** - List(string).  The tokens on which the transformation is applied.
                - **index** - List(Integer) or Integer.  The index or indices on which to operate to make the output.

            The return value of the method is simply some function of the input defined by the transformation method.
        """

        def transform_echo(self, tokens, index, **kwargs):
            """
                A simple identity transform of tokens[index].
            """
            return tokens[index]

        def transform_strip(self, tokens, index, **kwargs):
            """
                Strips the whitespace from tokens[index].
            """
            try:
                return tokens[index].strip()
            except:
                logging.error('transform_strip failed. Executing transform_echo ... ')
                return tokens[index]

        def transform_timestamp(self, tokens, index, **kwargs):
            """
                Expects a timestamp of the form "month/day/year hour:minute:second at tokens[index].
            """

            if re.search(r'([1-9]|1[0-2])/([1-9]|[1-3][0-9])/20[0-9][0-9].([0-9]|[1-2][0-9]):[0-9][0-9]:[0-9][0-9]', tokens[index]):

                try:

                    ts = ''

                    split_1 = tokens[index].split("/")
                    split_2 = split_1[2].split()[1]
                    split_2 = split_2.split(":")

                    # year
                    ts += split_1[2][0:4]

                    # month
                    if len(split_1[0]) == 1:
                        ts += "".join(['0',split_1[0]])
                    else:
                        ts += split_1[0]
                        # day
                    if len(split_1[1]) == 1:
                        ts += "".join(['0',split_1[1]])
                    else:
                        ts += split_1[1]

                    # hour
                    if len(split_2[0]) == 1:
                        ts += "".join(['0',split_2[0]])
                    else:
                        ts += split_2[0]

                    # minute
                    ts += split_2[1]
                    # second
                    ts += split_2[2]

                    # Shifts the timestamp according to hours_delta
                    if 'hours_delta' in kwargs:
                        ts_obj = tp.timestamp_to_obj(ts,1)
                        ts_obj += datetime.timedelta(hours=kwargs['hours_delta'])
                        ts = tp.timestamp_from_obj(ts_obj,1,3)

                    return ts

                except KeyError:
                    # logging.debug('Bad parse')
                    return tokens[index]

            else:
                return tokens[index]

        def transform_timestamp_date_parse(self, tokens, index):
            """
                Uses the data_parse method to extract a date object from tokens[index].
            """
            return str(date_parse(tokens[index]))

    class LineParseMethods():
        """
            Helper Class - Defines methods for processing lines of text primarily from log files.  Each method in this class takes one argument:

                - **line** - String.  Line text to process.

            The return value of the method is simply some function of the input defined by the transformation method.
        """

        def e3lm_log_parse(self, line):
            """
                Data Format:

                    https://meta.wikimedia.org/wiki/Research:Timestamp_position_modification/Clicktracking

                e.g. from /var/log/aft/click-tracking.log ::
                    enwiki ext.lastModified@1-ctrl1-impression	20120622065341	0	aLIoSWm5H8W5C91MTT4ddkHXr42EmTxvL	0	0	0	0	0

            """
            elems = line.split('\t')
            l = elems[0].split()
            l.extend(elems[1:])

            # in most cases the additional data will be missing - append a field here
            if len(l) < 11:
                l.append("no data")
            return l

        def e3_pef_log_parse(self, line):
            """
                Data Format:

                    https://meta.wikimedia.org/wiki/Research:Timestamp_position_modification/Clicktracking

                e.g. from /var/log/aft/click-tracking.log ::
                    enwiki ext.postEditFeedback@1-assignment-control	20120731063615	1	FGiANxyrmVcI5InN0myNeHabMbPUKQMCo	0	0	0	0	0	15667009:501626433
            """
            elems = line.split('\t')

            page_id = ''
            rev_id = ''
            user_hash = ''

            try:
                additional_data = elems[9]
                additional_data_fields = additional_data.split(':')

                if len(additional_data_fields) == 2:
                    page_id = additional_data_fields[0]
                    rev_id = additional_data_fields[1]
                    user_hash = ''

                elif len(additional_data_fields) == 3:
                    page_id = additional_data_fields[0]
                    rev_id = additional_data_fields[1]
                    user_hash = additional_data_fields[2]

            except:
                logging.info('No additional data for event %s at time %s.' % (elems[0], elems[1]))

            l = elems[0].split()
            l.extend(elems[1:9])

            l.append(user_hash)
            l.append(rev_id)
            l.append(page_id)

            # Append fields corresponding to `e3pef_time_to_milestone` and `e3pef_revision_measure`
            l.extend(['',''])

            return l

        def e3_acux_log_parse(self, line):
            """
                Process client and server side events.  Read to table, gather clean funnels.

                Dario says:

                    enwiki ext.accountCreationUX@2-acux_1-assignment	20121005000446	0	hGba7rOPWNmpc9lA7EQLnB5Nvb7ziBqoT	-1	0	0	0	0	frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC|http://en.wikipedia.org/w/index.php?title=Special:UserLogin&returnto=Miguel_(singer)&returntoquery=action%3Dedit
                    enwiki ext.accountCreationUX@2-acux_1-impression	20121005000447	0	hGba7rOPWNmpc9lA7EQLnB5Nvb7ziBqoT	-1	0	0	0	0	frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC|http://en.wikipedia.org/w/index.php?title=Special:UserLogin&returnto=Miguel_(singer)&returntoquery=action%3Dedit
                    enwiki ext.accountCreationUX@2-acux_1-submit	20121005000508	0	hGba7rOPWNmpc9lA7EQLnB5Nvb7ziBqoT	-1	0	0	0	0	frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC|http://en.wikipedia.org/w/index.php?title=Special:UserLogin&returnto=Miguel_(singer)&returntoquery=action%3Dedit|Mariellaknaus
                    enwiki ?event_id=account_create&user_id=17637802&timestamp=1349395510&username=Mariellaknaus&self_made=1&creator_user_id=17637802&by_email=0&userbuckets=%7B%22ACUX%22%3A%5B%22acux_1%22%2C2%5D%7D&mw_user_token=frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC&version=2

                    The sequence of events in this "clean" funnel is the following:

                    acux_1-assignment 	(client event)
                    acux_1-impression 	(client event)
                    acux_1-submit 		(client event)
                    account_create 		(server event)

                    The full specs of the events are here: https://meta.wikimedia.org/wiki/Research:Account_creation_UX/Logging

                    Duplicate events
                    Since users can go through complex funnels before submitting the account create form and generate errors after submitting, "clean"
                    funnels are going to be the exception, not the norm and we will need to collapse all funnels by token to extract meaningful metrics.
                    In other words, raw counts of -impression or -submit events will be meaningless and should not be used to calculate click through/conversion
                    rates prior to deduplication.

                    As a rule, there should only be one (server-side) account_create event associated with a token. The only exception is shared browsers
                    creating multiple accounts. In this case we will see multiple account_create events associated with the same token (which is persistent
                     across sessions and logins) but different user_id's.

                    Early stats
                    In the first hour since activation (23.30-00.30 UTC), we had 87 successful account creations from the acux_1 bucket vs 74
                    accounts from the control. This doesn't include users who by-passed the experiment by having JS disabled. The total number of
                    accounts registered in this hour on enwiki, per the logging table, is 180, so users who didn't get bucketed are 19 (i.e.
                    about 10% of all account registrations). This figure is higher than I expected so there might be other causes on top of JS
                    disabled that cause users to register without a bucket and that we may want to investigate.
            """
            line_bits = line.split('\t')
            num_fields = len(line_bits)

            # handle both events generated from the server and client side via ACUX.  Discriminate the two cases based
            # on the number of fields in the log

            if num_fields == 1:
                # SERVER EVENT - account creation
                line_bits = line.split()
                query_vars = cgi.parse_qs(line_bits[1])

                try:
                    # Ensure that the user is self made
                    if query_vars['self_made'][0]:
                        return [line_bits[0], query_vars['username'][0], query_vars['user_id'][0],
                            query_vars['timestamp'][0], query_vars['?event_id'][0], query_vars['self_made'][0],
                            query_vars['mw_user_token'][0], query_vars['version'][0], query_vars['by_email'][0],
                            query_vars['creator_user_id'][0]]
                    else:
                        return []

                except Exception:
                    return []

            elif num_fields == 10:
                # CLIENT EVENT - impression, assignment, and submit events
                fields = line_bits[0].split()
                fields.extend(line_bits[1:9])
                additional_fields = ['','']
                last_field = line_bits[9].split('|')

                if len(last_field) >= 2:
                    additional_fields[0] = last_field[0]
                    additional_fields[1] = last_field[1]

                elif len(last_field) == 1:
                    # Check whether the additional fields contain only a url
                    if urlparse(last_field[0]).scheme:
                        additional_fields[1] = last_field[0]
                    else:
                        additional_fields[0] = last_field[0]
                fields.extend(additional_fields)
                return fields
            return []
