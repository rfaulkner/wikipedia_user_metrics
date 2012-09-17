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
__date__ = "April 8th, 2011"


# Import python base modules
import sys
import MySQLdb
import datetime
import re
import logging
import gzip
import operator
# import numpy as np
from dateutil.parser import parse as date_parse
from abc import ABCMeta

# Import Analytics modules
import config.settings as projSet
import classes.TimestampProcessor as TP

sys.path.append(projSet.__wsor_msg_templates_home_dir__)
import umetrics.postings as post


# CONFIGURE THE LOGGER
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class DataLoader(object):
    """

        Base class for loading data from a specified source.  This class and its children also provide data processing functionality.

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

    def __init__(self, **kwargs):
        """ Constructor - Initialize class members and initialize the database connection  """

        self._results_ = (())
        self._col_names_ = None
        self._valid_ = False
                
        self.init_db(**kwargs) 
            

    def init_db(self, **kwargs):
        """
            Establishes a database connection.

            Parameters (\*\*kwargs):
                - **db**: string value used to determine the database connection

            Return:
                - empty.
        """

        if 'db' in kwargs:
            if kwargs['db'] == 'storage3':
                self._db_ = MySQLdb.connect(host=projSet.__db_storage3__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
            elif kwargs['db'] == 'db1008':
                self._db_ = MySQLdb.connect(host=projSet.__db_db1008__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
            elif kwargs['db'] == 'db1025':
                self._db_ = MySQLdb.connect(host=projSet.__db_db1025__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
            elif kwargs['db'] == 'db42':
                if 'db_instance' in kwargs:
                    self._db_ = MySQLdb.connect(host=projSet.__db_server_internproxy__, user=projSet.__user_internproxy__, db=kwargs['db_instance'], passwd=projSet.__pass_internproxy__)
                else:
                    self._db_ = MySQLdb.connect(host=projSet.__db_server_internproxy__, user=projSet.__user_internproxy__, db=projSet.__db_internproxy__, passwd=projSet.__pass_internproxy__, port=projSet.__db_port_internproxy__)
            elif kwargs['db'] == 'db1047':
                self._db_ = MySQLdb.connect(host=projSet.__db_db1047__, user=projSet.__user_internproxy__, db=projSet.__db_internproxy__, passwd=projSet.__pass_internproxy__, port=projSet.__db_port_internproxy__)
        else:            
            self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__, passwd=projSet.__pass__)
            # self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__)
        
        # Create cursor
        self._cur_ = self._db_.cursor()
     
    def close_db(self):
        self._cur_.close()
        self._db_.close()
        
    def establish_faulkner_conn(self):
        
        self.close_db()
        self._db_ = MySQLdb.connect(host=projSet.__db_server__, user=projSet.__user__, db=projSet.__db__, port=projSet.__db_port__)
        self._cur_ = self._db_.cursor()
    
    def establish_enwiki_conn(self):
        
        self.close_db()
        self._db_ = MySQLdb.connect(host=projSet.__db_server_internproxy__, user=projSet.__user_internproxy__, db=projSet.__db_internproxy__, port=projSet.__db_port_internproxy__, passwd=projSet.__pass_internproxy__)
        self._cur_ = self._db_.cursor()
        

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

            self._valid_ = True

            self._results_ =  self._cur_.fetchall()
            return self._results_

        except Exception as inst:
            
            self._db_.rollback()
            self._valid_ = False

            # logging.error('Could not execute: ' + SQL_statement)
            logging.error(str(type(inst)))      # the exception instance
            logging.error(str(inst.args))       # arguments stored in .args
            logging.error(inst.__str__())       # __str__ allows args to printed directly

            return -1
    

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


    def get_column_names(self):
        """
            Return the column names from the connection cursor (latest executed query)

            Return:
                - List(srting).  Column names from latest query results.
        """

        column_data = self._cur_.description
        column_names = list()
        
        for elem in column_data:
            column_names.append(elem[0])
            
        return column_names

    def cast_elems_to_string(self, input):
        """
            Casts the elements of a list or dictionary structure as strings.

            Parameters:
              - **input**: list or dictionary structure

            Return:
                - List(String), Dict(String), or Boolean.  Structure with string casted elements or boolean=False if the input was malformed.
        """

        if isinstance(input, list):
            output = list()
            for elem in input:
                output.append(str(elem))
        elif isinstance(input, dict):
            output = dict()
            for elem in input.keys():
                output[elem] = str(input[elem])
        else:
            return False

        return output

    def listify(self, list):
        """
            Turns input into a list

            Parameters:
              - **input**: List(), list to listify

            Return:
                - String.  List() or Boolean.  List with listified elements.
        """

        if not(isinstance(input,list)):
            return False
        else:
            output = list()
            for elem in list:
                output.append([elem])

            return output

    def stringify(self, input):
        """
            String processing tool - appends and prepends the string argument with double quotes

            Parameters:
              - **input**: list, dict, or obj argument

            Return:
                - String.  List(String), Dict(String), or Boolean.  Structure with stringified elements or boolean=False if the input was malformed.
        """

        if input is None:
            return False

        if isinstance(input, list):
            output = list()
            for elem in input:
                output.append(''.join(['"', str(elem), '"']))
        elif isinstance(input, dict):
            output = dict()
            for elem in input.keys():
                output[elem] = ''.join(['"', str(input[elem]), '"'])
        else:
            output = ''.join(['"', str(input), '"'])

        return output


    def histify(self, data, label_indices):
        """
            Turn a counts vector into a set of samples

            Parameters:
                - **data**: string argument
                - **label_indices**:

            Return:
                - List(Integer).  Histogram sample counts.
        """

        indices = range(len(data))
        hist_list = list()

        for index in indices:
            samples = [label_indices[index]] * data[index]            
            hist_list.extend(samples)
            
        return hist_list


    def dump_to_csv(self, **kwargs):
        """
            Data Processing - take **__results__** and dump into out.tsv in the data directory

            Parameters (\*\*kwargs):
                - **column_names** - list of strings storing the column names

            Return:
                - empty.
        """

        try:
            if 'column_names' in kwargs:
                column_names = kwargs['column_names']
            else:
                column_names = self.get_column_names()
            
            logging.info('Writing results to: ' + projSet.__data_file_dir__ + 'out.tsv')    
            output_file = open(projSet.__data_file_dir__ + 'out.tsv', 'wb')
            
            # Write Column headers
            for index in range(len(column_names)):
                if index < (len(column_names) - 1):
                    output_file.write(column_names[index] + '\t')
                else:
                    output_file.write(column_names[index] + '\n')
                    
            # Write Rows                
            for row in self._results_:
                for index in range(len(column_names)):
                    if index < (len(column_names) - 1):
                        output_file.write(str(row[index]) + '\t')
                    else:
                        output_file.write(str(row[index]) + '\n')                    
            
            output_file.close()
            
        except Exception as e:
            logging.error(e.message)


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
            for i in range(len(elems)):
                elems[i] = MySQLdb._mysql.escape_string(elems[i])
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
            except:
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
                              create_table = False, log_out=False, log_velocity=10000, user_db='rfaulk',
                              regex_list=None, neg_regex_list=None, header=True, separator='\t'):
        """
            Populates or creates a table from a .xsv file.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **create_sql** - String.  Contains the SQL create statement for the table (if necessary).
                - **table_name** - String.  Name of table to populate.
                - **parse_function** - Method Pointer.  Method that performs line parsing (see helper class TransformMethod)
                - **create_table** - Boolean.  Flag that indicates whether table creation is to occur.
                - **log_out** - Boolean.  Flag indicating whether to output logging.
                - **log_velocity** - Integer. Determines the frequency of logging.
                - **user_db** - String. Database instance.
                - **regex_list** - List(string).  List of inclusive regex strings over which each line of input will be conditioned.
                - **neg_regex_list** - List(string).  List of exclusive regex strings over which each line of input will be conditioned.
                - **header**: Boolean.  Flag indicating whether the file has a header.
                - **separator**: String.  The separating character in the file.  Default to tab.

            Return:
                - empty.
        """

        if re.search("\.gz", filename):
            file_obj = gzip.open(projSet.__data_file_dir__ + filename, 'rb')
        else:
            file_obj = open(projSet.__data_file_dir__ + filename, 'r')

        if create_table:
            try:
                self.execute_SQL("drop table if exists `rfaulk`.`%s`" % table_name)
                self.execute_SQL(create_sql)
            except:
                logging.error('Could not create table: %s' % create_sql)
                return


        # Get column names

        self.execute_SQL('select * from `%s`.`%s` limit 1' % (user_db, table_name))
        column_names = self.get_column_names()
        column_names_str = self.format_comma_separated_list(column_names, include_quotes=False)


        # Prepare SQL syntax
        insert_sql = 'insert into `%(user_db)s`.`%(table_name)s` (%(column_names)s) values ' % {'table_name' : table_name, 'column_names' : column_names_str, 'user_db' : user_db}


        # Crawl the log line by line
        # insert the contents of each line into the slave table

        count = 1

        if header:
            file_obj.readline()

        line = file_obj.readline().strip()
        while line != '':

            # First evaluate whether there are any regex's n which to test the string
            # Skip to the next line if the condition is not satisfied

            # patterns that must be present
            include_line = True

            if isinstance(regex_list, list):
                for r in regex_list:
                    if not(re.search(r, line)):
                        line = file_obj.readline().strip()
                        include_line = False
                        break


            # patterns that must be not be present
            if isinstance(neg_regex_list, list):
                for r in neg_regex_list:
                    if re.search(r, line):
                        line = file_obj.readline().strip()
                        include_line = False
                        break

            if not(include_line):
                continue

            # Check condition for optional logging
            if log_out and count % log_velocity == 0:
                logging.info('Current line: %s\nProcessed %s lines' % (line, str(count)))

            # Parse input line
            if parse_function == None:
                insert_field_str = self.format_comma_separated_list(line.split(separator))
            else:
                insert_field_str = self.format_comma_separated_list(parse_function(line))

            # Only add the record
            if len(insert_field_str.split(',')) == len(column_names):
                insert_sql += '(%s), ' % insert_field_str
                #else:
            #    logging.info('Skipped line: %s' % insert_field_str)

            count += 1
            line = file_obj.readline().strip()


        # Perform insert
        logging.info('Inserting %s records into %s' % (str(count), str(table_name)))

        insert_sql = insert_sql[:-2]
        self.execute_SQL(insert_sql)



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


    def remove_elems_from_xsv(self, filename, elems, index, separator='\t', header=True, inclusive=True, opt_ext='.rem', regex_pattern=None):
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
                        if not(regex_pattern, re.search(tokens[index])):
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


    def transform_xsv(self, filename, index_generator_methods=[], separator_from='\t', separator_to='\t', outfile = None, header=False, **kwargs):
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
        results = self.execute_SQL(sql)

        for row in results:
            line_str = ''
            for elem in row:
                line_str = line_str + str(elem) + separator
            line_str = line_str[:-1] + '\n'
            file_obj_out.write(line_str)
        file_obj_out.close()


    # Helper classes
    # ==============

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
                        ts_obj = TP.timestamp_to_obj(ts,1)
                        ts_obj += datetime.timedelta(hours=kwargs['hours_delta'])
                        ts = TP.timestamp_from_obj(ts_obj,1,3)

                    return ts

                except:
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

            l.append(page_id)
            l.append(rev_id)
            l.append(user_hash)

            # Append fields corresponding to `e3pef_time_to_milestone` and `e3pef_revision_measure`
            l.extend(['',''])

            return l



class ExperimentsLoader(DataLoader):
    """
        This class contains methods and classes for retrieving and processing experimental data.
    """

    #    "`e3lm_event` varbinary(255) NOT NULL DEFAULT ''," +\
    E3_LM_SLAVE_TABLE = "create table `rfaulk`.`e3_last_modified_iter1_log_data` (" +\
    "`e3lm_project` varbinary(255) NOT NULL DEFAULT ''," + \
    "`e3lm_event` varbinary(255) NOT NULL DEFAULT ''," + \
    "`e3lm_timestamp` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_user_category` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_user_token` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_namespace` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_lifetime_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_6month_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_3month_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_last_month_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3lm_additional_data` varbinary(255) NOT NULL DEFAULT '')"

    E3_PEF_SLAVE_TABLE = "create table `rfaulk`.`e3_pef_iter1_log_data` (" +\
    "`e3pef_project` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_event` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_timestamp` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_user_category` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_user_token` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_namespace` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_lifetime_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_6month_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_3month_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_last_month_edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_page_id` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_rev_id` varbinary(255) NOT NULL DEFAULT ''," +\
    "`e3pef_user_hash` varbinary(255) NOT NULL DEFAULT '')"

    E3_PEF_NS_TABLE = "create table `rfaulk`.`e3_pef_iter1_ns` (" +\
                         "`user_id` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`namespace` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`revisions` varbinary(255) NOT NULL DEFAULT '')"

    E3_PEF_EC_TABLE = "create table `rfaulk`.`e3_pef_iter1_editcount` (" +\
                      "`user_id` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`edit_count` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`byte_count` varbinary(255) NOT NULL DEFAULT '')"

    E3_PEF_TTT_TABLE = "create table `rfaulk`.`e3_pef_iter1_timetothreshold` (" +\
                      "`user_id` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`time_minutes` varbinary(255) NOT NULL DEFAULT '')"

    E3_PEF_BA_TABLE = "create table `rfaulk`.`e3_pef_iter1_bytesadded` (" +\
                       "`user_id` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`bytes_added_net` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`bytes_added_abs` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`bytes_added_pos` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`bytes_added_neg` varbinary(255) NOT NULL DEFAULT ''," +\
                      "`edit_count` varbinary(255) NOT NULL DEFAULT '')"

    E3_PEF_WARN_TABLE = "create table `rfaulk`.`e3_pef_iter1_warn` (" +\
                         "`user_name` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`registration` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`warns_before` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`warns_after` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`first_warn_before` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`last_warn_before` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`first_warn_after` varbinary(255) NOT NULL DEFAULT ''," +\
                         "`last_warn_after` varbinary(255) NOT NULL DEFAULT '')"


    SQL_CUSTOM_POSTINGS = 'SELECT r.rev_id, r.rev_timestamp as timestamp, r.rev_comment, r.rev_user AS poster_id, r.rev_user_text AS poster_name, ' + \
                          'REPLACE(p.page_title, "_", " ") AS recipient_name FROM revision r INNER JOIN page p ON r.rev_page = p.page_id ' + \
                          'WHERE rev_timestamp BETWEEN "%(start)s" AND "%(end)s" AND page_namespace = %(page_namespace)s'

    E3_TWINKLE_PROD_TEST_FILE = "prod_test_tags.tsv"
    E3_TWINKLE_PROD_CONTROL_FILE = "prod_control_tags.tsv"

        
    def __init__(self):     
        # Call constructor of parent
        DataLoader.__init__(self, db='db42')



    def filter_bots(self, users):
        """
            Filter bots from a user list.

            Parameters:
                - **users** - list of user names to be matched on the bot REGEX

            Return:
                - List(string).  Filtered results.
        """

        logging.info('Finding bots in list')

        bot_regex = r'[Bb][Oo][Tt]'
        non_bots = list()
        for user in users:
            if not(re.search(bot_regex, user)):
                non_bots.append(user)
        return non_bots


    def filter_blocks(self, users, start, end):
        """
            Filter blocked users from a list for a given time range.  The method determines all users in blocked in the given timeframe by querying
            the *logging* table.  Returns all users in the input list that are ***not*** blocked.

            Parameters:
                - **users** - list of user names to be checked for blocks
                - **start** - start timestamp of the block period
                - **end** - end timestamp of the block period

            Return:
                - List(tuple).  Query results.
        """

        value = ','.join('"???"'.join(users).split('???')[1:-2])
        sql = 'select log_title, max(log_timestamp) from logging where log_title in (%s) and log_timestamp >= "%s" and log_timestamp < "%s" and log_action = "block" group by 1;'
        sql = sql % (value, start, end)
        
        logging.info('Finding blocked users in list')
        return self.execute_SQL(sql)
        

    def get_user_email(self, users):
        """
            Get user emails from a list of users.  Queries the *user* table.  Returns a list of tuples containing user name and email.

            Parameters:
                - **users** - list of user names for whom to retrieve email addresses (where available)

            Return:
                - List(tuple).  Query results.
        """

        logging.info('Finding user emails')
        
        value = ','.join('"???"'.join(users).split('???')[1:-2])
        sql = 'select user_name, user_email from user where user_name in (%s) and user_email_authenticated IS NOT NULL;'
        sql = sql % value
        
        return self.execute_SQL(sql)


    def filter_by_edit_counts(self, rows, index, lower_threshold):
        """
            Filters a list of users by edit count.  Returns the rows meeting the minimum threshold criteria.

            Parameters:
                - **rows** - list of tuples containing user data (SQL output)
                - **index** - index of edit count
                - **lower_threshold** - minimum value of edit count

            Return:
                - List(tuple).  Filtered results.
        """

        new_rows = list()
        for row in rows:
            if int(row[index]) >= lower_threshold:
                new_rows.append(row)

        return new_rows


    def get_user_revisions(self, users, start_time, end_time):
        """
            Select all revisions in a given time period from the revision table for the given list of users.  Queries the *revision* table.
            Returns two lists, the query fields and the results of the query.

            Parameters:
                - **users** - list of users on which to condition the query
                - **start_time** - earliest revision timestamp
                - **end_time** - latest revision timestamp

            Return:
                - List(string).  Column names.
                - List(tuple).  Query results.
        """
        
        users_str = self.format_comma_separated_list(users)
        
        sql = 'select rev_timestamp, rev_comment, rev_user, rev_user_text, rev_page from revision ' + \
        'where rev_timestamp >= "%(start_timestamp)s" and rev_timestamp < "%(end_timestamp)s" and rev_user_text in (%(users)s)'
        sql = sql % {'start_timestamp' : start_time, 'end_timestamp' : end_time, 'users' : users_str}        
                      
        self._results_ = self.execute_SQL(sql)
        col_names = self.get_column_names()
        
        return col_names, self._results_


    def get_user_page_ids(self, filename, parse_method, header=True):
        """
            Get a list of user page ids embedded in a text file.  A parse method is used to extract the ids from each line of the input file.
            This method queries the page table conditioned on UserTalk namespace and the user list generated from the file.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **parse_method** - Method Pointer.  Method that handles extracting content from each line (see helper class XSVParseMethods).
                - **header** - Boolean.  Flag indicating whether the file has a header.

            Return:
                - List.  List of elements parsed from each line of the input.
        """

        try:

            users = self.extract_pattern_from_text_file(filename, parse_method, header=header)
            user_list_str = self.format_comma_separated_list(users)

            # get page ids for eligible - format csv list
            sql = "select page_id from page where page_namespace = 3 and page_title in (%s);" % user_list_str

            result = self.execute_SQL(sql)

            id_list = list()
            for row in result:
                id_list.append(str(row[0]))

        except Exception():
            logging.error('Could not parse ids from: %s' % filename)
            return []

        return id_list
        # user_list_str = self.format_comma_separated_list(id_list)
        # return "".join(["rev_page in (", user_list_str, ")"])



class SharedIPLoader(ExperimentsLoader):
    """
        Class for processing data on the SharedIP Experiment
    """

    def __init__(self):
        """ Call constructor of parent """
        ExperimentsLoader.__init__(self)

    def create_first_rev_date(self):
        """
            SHARED IP :: Create a table that stores first revision date by user
        """
        sql_create = """
            CREATE TABLE rfaulk.`first_revision_by_user` (
              `fru_user_text` varbinary(255) NOT NULL DEFAULT '',
              `fru_timestamp` varbinary(14) NOT NULL DEFAULT '',
              UNIQUE KEY `fru_user_text` (`fru_user_text`),
              KEY `fru_timestamp` (`fru_timestamp`)
            );
        """
        
        drop = 'drop table if exists rfaulk.first_revision_by_user'
        
        logging.info('Creating first_revision_by_user table.')
        
        self.execute_SQL(drop)
        self.execute_SQL(sql_create)

    def insert_into_first_rev_date_from_shared_ip(self):
        """
            SHARED IP ::  Create a table that stores first revision date by user
        """
        
        shared_ip_users = self.get_archived_shared_ip_users()
        where_str = self.format_clause(shared_ip_users, 0, self.OR, 'rev_user_text')
        
        logging.info('Inserting earliest revision date for %s ips ... ' % len(shared_ip_users))

        sql = 'insert rfaulk.first_revision_by_user (fru_user_text, fru_timestamp) ' + \
        'select rev_user_text as fru_user_text, min(rev_timestamp) as fru_timestamp from enwiki.revision where %(where_str)s group by 1' 
        
        sql = sql % {'where_str' : where_str}
        self.execute_SQL(sql)

    def generate_counts_before_and_after_archiving(self):
        """
            SHARED IP :: Create a table that stores first revision date by user

            https://en.wikipedia.org/wiki/User:SharedIPArchiveBot
            https://meta.wikimedia.org/wiki/User:Staeiou/Journal/SharedIP
        """
        
        time_bounds_before = ['20101220000000', '20110213000000']
        time_bounds_after = ['20111220000000', '20120213000000']
        
        shared_ip_users = self.get_archived_shared_ip_users()
        
        # Get rev counts from sharedIP table
        logging.info('Building rev counts for archived shared ips ...')
        
        sql_ips_before = 'select rev_user_text, count(*) as revs from revision where (rev_timestamp >= "%(time_1)s" and rev_timestamp < "%(time_2)s") and (%(where_str)s) group by 1;'        
        sql_ips_after = 'select rev_user_text, count(*) as revs from revision where (rev_timestamp >= "%(time_3)s" and rev_timestamp < "%(time_4)s") and (%(where_str)s) group by 1;'
            
        where_str = self.format_clause(shared_ip_users, 0, self.OR, 'rev_user_text')
        
        sql_ips_before = sql_ips_before % {'time_1': time_bounds_before[0], 'time_2': time_bounds_before[1], 'where_str' : where_str}
        sql_ips_after = sql_ips_after % {'time_3': time_bounds_after[0], 'time_4': time_bounds_after[1], 'where_str' : where_str}
        shared_ip_users = self.get_archived_users_after_first_rev(shared_ip_users, time_bounds_before[0])
        
        # Create table rfaulk.shared_ip
        results_before = self.execute_SQL(sql_ips_before)
        results_after = self.execute_SQL(sql_ips_after)
        
        # Get all rows from rfaulk.shared_ip
        # 
        
        logging.info('Building rev counts for archived shared ips ...')
        
        results_before_hash = dict()
        results_after_hash = dict()
        all_results_list = list()
        
        for row in results_before:
            results_before_hash[row[0]] = int(row[1])

        for row in results_after:
            results_after_hash[row[0]] = int(row[1])
        
        for row in shared_ip_users:
            user = row[0]
            if user in results_before_hash.keys() and user in results_after_hash.keys():
                all_results_list.append((user, results_before_hash[user], results_after_hash[user]))
            elif user in results_before_hash.keys():
                all_results_list.append((user, results_before_hash[user], 0))
            elif user in results_after_hash.keys():
                all_results_list.append((user, 0, results_after_hash[user]))
            else:
                all_results_list.append((user, 0, 0))
                
            self._results_ = all_results_list
            
        # Dump to csv
        self.dump_to_csv(column_names=['Shared IP', 'Revs Before', 'Revs After'])

    def get_archived_shared_ip_users(self):
        """
            SHARED IP :: Get a list of the archived IPs in staeiou.sharedip_all
        """

        # Get users from sharedIP table
        logging.info('Getting users from staeiou.sharedip_all ...')
                
        sql = 'select distinct(page_title) as shared_IP from staeiou.sharedip_all where isarchived = 1;'
        
        return self.execute_SQL(sql)

    def get_archived_users_after_first_rev(self, users, start_time):
        """
            SHARED IP :: Get a list of the archived IPs in staeiou.sharedip_all
        """
        
        logging.info('Getting users from staeiou.sharedip_all ...')
        
        where_str = self.format_clause(users, 0, self.OR, 'fru_user_text')
        sql = 'select fru_user_text from rfaulk.first_revision_by_user where %(where_str)s and fru_timestamp > "%(start_time)s";'
        sql = sql % {'where_str' : where_str, 'start_time' : start_time}
                
        return self.execute_SQL(sql)
    

class TwinkleLoader(ExperimentsLoader):
    """
        Class for processing data on the Twinkle Experiments
    """

    def __init__(self):     
        """ Call constructor of parent """
        ExperimentsLoader.__init__(self)

    def find_prod_tag(self):
        """
            TWINKLE_PROD - Get PROD tag edits
        """
        prod_test_tags_tsv = open(projSet.__data_file_dir__ + self.E3_TWINKLE_PROD_TEST_FILE,'wb')
        prod_control_tags_tsv = open(projSet.__data_file_dir__ + self.E3_TWINKLE_PROD_CONTROL_FILE,'wb')
        
        prod_test_tags_tsv.write("\t".join(["user", "rev_timestamp", "rev_id"]) + '\n')
        prod_control_tags_tsv.write("\t".join(["user", "rev_timestamp", "rev_id"]) + '\n')
        
        # PROD_tag_regex = "{{Proposed deletion/dated.*}}"
        PROD_tag_regex = "{{[Pp]roposed [Dd]eletion"
        sql_all_revs_for_user = 'select rev_timestamp, rev_id from revision where rev_user_text = "%(user)s" and rev_timestamp >= "%(ts)s"'
                
        # import metrics from prod tests - 78 = control, 79 = test
        prod_control_users = dict()
        prod_test_users = dict()
        prod_control_tags = dict()
        prod_test_tags = dict()
        
        file_control = open("".join([projSet.__message_templates_home__, 'output/metrics_1109_1209_z78_blocks.tsv']), 'r')
        file_test = open("".join([projSet.__message_templates_home__, 'output/metrics_1109_1209_z79_blocks.tsv']), 'r')
        
        file_control.readline() # skip the header
        line = file_control.readline()
        while (line != ''):
            elems = line.split('\t')
            prod_control_users[elems[0]] = {'timestamp' : elems[1]} 
            line = file_control.readline()
        
        file_test.readline() # skip the header
        line = file_test.readline()
        while (line != ''):
            elems = line.split('\t')
            prod_test_users[elems[0]] = {'timestamp' : elems[1]}
            line = file_test.readline()
                
        # look in the revision text to see if the tag was removed - use aaron's WPIAPI class
        logging.info("Connecting to API @ %s." % 'http://en.wikipedia.org/w/api.php')
        api = post.WPAPI('http://en.wikipedia.org/w/api.php')

        dl = DataLoader(db='db1047')
        
        # For control group look through all users in list and determine  if any removed the PROD tag
        for user in prod_control_users:
            sql = sql_all_revs_for_user % {'user' : user, 'ts' : prod_control_users[user]['timestamp']}
            revs = dl.execute_SQL(sql)
            
            prod_control_tags[user] = list()
            
            logging.info('Getting rev text for CONTROL user: %s' % user)
            logging.info('Executing "%(sql)s"' % {'sql' : sql})
            
            for rev in revs:
                
                rev_id = rev[1]
                rev_ts = rev[0]
                
                try:
                    message = api.getAdded(rev_id)                
                except:
                    logging.error('Could not generate rev text for user / revision: %s, %s' % (user, str(rev_id)))
                    message = ""
                    pass
                
                if re.search(PROD_tag_regex, message):
                    prod_control_tags[user].append((rev_id, rev_ts))
                    logging.info('Found rev_id, rev_ts: %s, %s' % (rev_id, rev_ts))

            # write to the tsv
            if len(prod_control_tags[user]) > 0:
                for elem in prod_control_tags[user]:
                    prod_control_tags_tsv.write("\t".join([user, str(elem[0]), str(elem[1])]) + '\n')
            else:
                prod_control_tags_tsv.write("\t".join([user, "None", "None\n"]))

        prod_control_tags_tsv.close()
        
        # For test group look through all users in list and determine  if any removed the PROD tag
        for user in prod_test_users:
            sql = sql_all_revs_for_user % {'user' : user, 'ts' : prod_test_users[user]['timestamp']}
            revs = dl.execute_SQL(sql)
            
            prod_test_tags[user] = list()
            
            logging.info('Getting rev text for TEST user: %s' % user)
            logging.info('Executing "%(sql)s"' % {'sql' : sql})
            
            for rev in revs:
                
                rev_id = rev[1]
                rev_ts = rev[0]
                
                try:
                    message = api.getAdded(rev_id)
                except:
                    logging.error('Could not generate rev text for user / revision: %s, %s' % (user, str(rev_id)))
                    message = ""
                    pass
                
                if re.search(PROD_tag_regex, message):
                    prod_test_tags[user].append((rev_id, rev_ts))
                    logging.info('Found rev_id, rev_ts: %s, %s' % (rev_id, rev_ts))
            
            # write to the tsv
            if len(prod_test_tags[user]) > 0:
                for elem in prod_test_tags[user]:
                    prod_test_tags_tsv.write("\t".join([user, str(elem[0]), str(elem[1])]) + '\n')
            else:
                prod_test_tags_tsv.write("\t".join([user, "None", "None\n"]))


        prod_test_tags_tsv.close()
            
        return prod_test_tags, prod_control_tags

    def parse_prod_revisions(self):
        """ TWINKLE_PROD - Parse PROD csv created by """

        prod_test_tags_tsv = open(projSet.__data_file_dir__ + self.E3_TWINKLE_PROD_TEST_FILE,'r')
        prod_control_tags_tsv = open(projSet.__data_file_dir__ + self.E3_TWINKLE_PROD_CONTROL_FILE,'r')
        
        total_test = 0
        total_control = 0

        response_test = 0
        response_control = 0
        
        # Crawl through test users
        user_hash = dict()
        
        prod_test_tags_tsv.readline()
        line = prod_test_tags_tsv.readline()
        
        while line != '':
            elems = line.split('\t')
            if not(elems[0] in user_hash):
                if elems[1] == "None":
                    total_test +=  1
                else:
                    total_test +=  1
                    response_test +=  1
                user_hash[elems[0]] = True
                
            line = prod_test_tags_tsv.readline()
        prod_test_tags_tsv.close()
        
        # Crawl through control users
        user_hash = dict()
        
        prod_control_tags_tsv.readline()
        line = prod_control_tags_tsv.readline()
        
        while line != '':
            elems = line.split('\t')
            if not(elems[0] in user_hash):
                if elems[1] == "None":
                    total_control +=  1
                else:
                    total_control +=  1
                    response_control +=  1
                user_hash[elems[0]] = True

            line = prod_control_tags_tsv.readline()
        prod_control_tags_tsv.close()
    
        print " ".join(['Total test :', str(total_test)])
        print " ".join(['Resonse test :', str(response_test)])
        print " ".join(['Total control :', str(total_control)])
        print " ".join(['Response control :', str(response_control)])


class NecromancyLoader(ExperimentsLoader):
    """
        Class for processing data on the Necromancy Experiment.  The definition of the experiment can be found at:

            `http://meta.wikimedia.org/wiki/Research:Necromancy`

        The aim is to contact users that maintained a certain amount active for a given initial period followed by a subsequent
        period of complete inactivity.  It is then determined whether any email information was recorded for eligible users, and if so,
        they are given a "call to action" to return to the project to contribute further.

        Below is an example of how a class object can be used to generate a list of Necromancy users: ::

            >>> nl = DL.NecromancyLoader()
            >>> ref_datetime = datetime.datetime(year=2012,month=9,day=1)
            >>> outfilename = nl.find_idle_users(ref_datetime,30,30)
            >>> nl.read_idle_users(outfilename,5)
    """

    def __init__(self):
        """ Call constructor of parent """
        ExperimentsLoader.__init__(self)

    def read_idle_users(self, filename, threshold):
        """
            Takes the output from *find_idle_users* and filters the results based on the main namespace edit count threshold.  Bots are filtered
            and email data, where available, is appended.  *There is, as yet, not built in functionality for vandal or sock-puppet filtering.*

            Parameters:
                - **filename** - String.  Filename of file containing output from *find_idle_users*.
                - **threshold** - Integer.  Minimum number fo main namespace edits on which to consider user.

            Return:
                - empty.
        """

        infile = open(projSet.__data_file_dir__ + filename, 'rb')
        outfile = open(projSet.__data_file_dir__ + filename + '.out', 'w')
        
        # Read the file and filter based on edit count
        rows = list()
        infile.readline() # ignore the first line header
        line = infile.readline()
        while line != '':
            elems = line.split('\t') 
            rows.append(elems)
            line = infile.readline()
        
        infile.close()
        filtered_rows = self.filter_by_edit_counts(rows, 2, threshold)
        
        users = list()
            
        # Build user list
        for row in filtered_rows:            
            users.append(row[1])

        
        # remove bots, blocked users, and find emails
        users_no_bots = self.filter_bots(users)
        # results_no_blocks = self.filter_blocks(users, start_ts, end_ts)
        email_results = self.get_user_email(users)        
        
        blocked_users = list()
        
        user_emails = dict()
        for row in email_results:
            user_emails[row[0]] = row[1]
        for user in users:
            if not(user in user_emails.keys()):
                user_emails[user] = ''

                
        # Compose the new list of users
        infile = open(projSet.__data_file_dir__ + filename, 'rb')
        
        line = infile.readline()
        outfile.write('\t'.join([line[:-1], 'user_email', '\n']))
        line = infile.readline() 
        
        while line != '':
            elems = line.split('\t')
            if elems[1] in users_no_bots and not(elems[1] in blocked_users) and user_emails[elems[1]] != '':
                outfile.write("\t".join([line[:-1], user_emails[elems[1]], '\n']))
            line = infile.readline()
            
        outfile.close()
        infile.close()


    def find_idle_users(self, ref_datetime, length_of_post, length_of_pre):
        """
            Generate a list of users and their edit counts and latest edit falling within a given period defined by the parameters.

            Parameters:
                - **length_of_pre** - Integer.  Length in days of  period.
                - **length_of_post** - Integer.  Length in days of idle period.
                - **ref_datetime** - datetime.  The reference datetime that

            Return:
                - String.  Formatted comma separated string of the list elements
        """

        start = ref_datetime + datetime.timedelta(days=-(length_of_post + length_of_pre))
        end = ref_datetime + datetime.timedelta(days=-(length_of_post))
        
        # Get all users above the edit threshold
        start_ts = date_parse(start)
        end_ts = date_parse(end)

        # sql = 'select rev_user, rev_user_text, count(*) as edits, max(rev_timestamp) from revision where rev_timestamp >= "%s" and rev_timestamp < "%s" group by 1,2'
        sql = 'select rev_user, rev_user_text, count(*) as edits, max(rev_timestamp) as latest_edit ' + \
              'from (select rev_page, rev_user, rev_user_text, rev_timestamp from revision where rev_timestamp >= "%s" and rev_timestamp < "%s" group by 1,2) as revs ' + \
              'join (select page_id from page where page_namespace = 0) as pg on revs.rev_page = pg.page_id group by 1,2'
        sql_formatted = sql % (start_ts, end_ts)
        
        logging.info('Executing: %s' % sql_formatted)
        results = self.execute_SQL(sql_formatted)
        
        user_list_pre = dict()
        for row in results:
            user_list_pre[row[0]] = (row[1], row[2], row[3])
                
                            
        # Find all editors in the post period 
        sql_formatted = sql % (end_ts, TP.timestamp_from_obj(curr_obj, 1, 0))
        
        logging.info('Executing: %s' % sql_formatted)
        results = self.execute_SQL(sql_formatted)
        
        user_list_post = dict()
        for row in results:
            user_list_post[row[0]] = (row[1], row[2])
            
        
        idle_user_list = user_list_pre.keys()
        user_post_keys = user_list_post.keys()
        
        # Remove users that edited in the post period
        for key in user_list_pre:
            if key in user_post_keys:
                idle_user_list.remove(key)

        # Dump to tsv
        outfilename = 'idle_users_' + str(start) + '.tsv'
        outfile = open(projSet.__data_file_dir__ + outfilename, 'w')
        outfile.write("\t".join(['userid', 'username', 'count_pre', 'latest_timestamp']) + '\n')
        for elem in idle_user_list:            
            outfile.write("\t".join([str(elem), str(user_list_pre[elem][0]), str(user_list_pre[elem][1]), TP.timestamp_convert_format(str(user_list_pre[elem][2]), 1, 2)]) + '\n')
        outfile.close()

        return outfilename
        # return idle_user_list, user_list_pre, user_list_post


class TableLoader(DataLoader):
    """
        Base class for providing MySQL table access.  Inherits DataLoader.  This class is abstract (not enforced) and implements the
        Template Method design pattern.

    """

    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        """
            Constructor. Call constructor of parent class.
        """
        self._table_name_ = 'meta'
        DataLoader.__init__(self, **kwargs)

    def record_exists(self, **kwargs):
        """
            Returns a boolean value reflecting whether a record exists in the table.
        """
        return
    
    def insert_row(self, record_list, **kwargs):
        """
            Try to insert a new record (s)into the table.
        """

        return

    def delete_row(self, **kwargs):
        """
            Try to delete a record() from the table.
        """

        return
    
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
        except:
            logging.info('Failed to assign all values.')

        logging.info('%s rows successfully updated in %s' % (str(rows_updated), self._table_name_))


    def build_table_query(self, select_fields, table_name, where_fields=[], where_ops=[], group_fields=[], order_fields=[]):
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
                where_str = ''
                           
            if order_fields:
                order_str = 'order by '
                for field in order_fields:
                    order_str = field + ','
                order_str = order_str[:-1]
            else:
                where_str = ''
                            
            sql = '%s from %s %s %s %s' % (select_str, table_name, where_str, group_str, order_str)        
            
        except:
            logging.info('Could not build query for %s: ' % table_name)
            sql = ''
            
        return sql


class PageCategoryTableLoader(TableLoader):
    """
        Subclass of TableLoader that provides custom access to *db42.pmtpa.wmnet.rfaulk.page_category*.  The definition follows: ::

            +----------------+-----------------+------+-----+---------+-------+
            | Field          | Type            | Null | Key | Default | Extra |
            +----------------+-----------------+------+-----+---------+-------+
            | page_id        | int(8) unsigned | YES  | MUL | NULL    |       |
            | page_title     | varbinary(255)  | YES  | MUL | NULL    |       |
            | category       | varbinary(255)  | YES  |     | NULL    |       |
            | category_value | varbinary(255)  | YES  | MUL | NULL    |       |
            +----------------+-----------------+------+-----+---------+-------+

        This table stores the top-level categorization information as determined by CategoryLoader class.
    """

    def __init__(self, **kwargs):
        """
            Call constructor of parent """


        TableLoader.__init__(self, **kwargs)
        self._table_name_ = 'rfaulk.page_category'

        self._top_level_categories_ = ['Mathematics',
         'People',
         'Science',
         'Law',
         'History',
         'Culture',
         'Politics',
         'Technology',
         'Education',
         'Health',
         'Business',
         'Belief',
         'Humanities',
         'Society',
         'Life',
         'Environment',
         'Computers',
         'Arts',
         'Language',
         'Places']

    def __del__(self):
        self.close_db()


    def get_article_categories_by_page_ids(self, page_id_list):
        """
            Retrieve article top-level category for a list of page ids

                - Parameters:

                    - **page_id_list**: List(string or numeric).  List of IDs from the `enwiki`.`page` table.

                - Return:

                    Dictionary(key=string).  **key**: Top-level categories name; **value**: count of occurrence as most relevant category among page IDs

        """

        page_id_str = ''
        for id_ in page_id_list:
            page_id_str = page_id_str + 'page_id = %s or ' % str(id_)
        page_id_str = page_id_str[:-4]
        
        page_id_str = MySQLdb._mysql.escape_string(page_id_str)
        sql = 'select category from faulkner.page_category where %s' % page_id_str
        
        results = self.execute_SQL(sql)
        
        category_counts = dict()
        for category in self._top_level_categories_:
            category_counts[category] = 0
            
        for row in results:
            category = row[0]
            category = category.split(',')[0]
            category_counts[category] = category_counts[category] + 1            
            
        return category_counts


    def get_normalized_category_counts(self, page_id_list):
        """
            Computes relative portions of categories among all pages and then compares those amounts to the relative persistence of categories
            among a sample of pages.  A category score is computed from this.

                - Parameters:

                    - **page_id_list**: List(string or numeric).  List of IDs from the `enwiki`.`page` table.

                - Return:
                    - Dictionary.  **key**: top-level category names (string); **value**: floating point value of
        """

        norm_results = NormalizedCategoryScoresTableLoader().get_all_rows()        
        norm_cats = dict()
        
        for row in norm_results:
            category = NormalizedCategoryScoresTableLoader().get_record_field(row, 'category')
            portion = NormalizedCategoryScoresTableLoader().get_record_field(row, 'portion')
            norm_cats[category] = portion
        
        category_counts = self.get_article_vector_counts(page_id_list)
        cat_count_total = 0.0
        
        for category in category_counts:
            cat_count_total = cat_count_total + category_counts[category]
        for category in category_counts:
            category_counts[category] = float(category_counts[category]) / cat_count_total

        category_score = dict()
        for category in norm_cats:
            try:
                category_score[category] = (category_counts[category] - norm_cats[category]) / norm_cats[category] * 100.0
            except:
                category_score[category] = -1.0
                pass
            
        return category_score


class NormalizedCategoryScoresTableLoader(TableLoader):
    """
        Subclass of TableLoader that provides custom access to *db42.pmtpa.wmnet.rfaulk.normalized_category_scores*.  The definition follows: ::

            +----------------+----------------+------+-----+---------+-------+
            | Field          | Type           | Null | Key | Default | Extra |
            +----------------+----------------+------+-----+---------+-------+
            | category       | varbinary(255) | YES  |     | NULL    |       |
            | category_total | bigint(21)     | NO   |     | 0       |       |
            | portion        | decimal(25,6)  | YES  |     | NULL    |       |
            +----------------+----------------+------+-----+---------+-------+

        This table stores a row for each top-level category containing the total number of times that category appears as the most closely related
        category for pages and the relative portion of that category among all other top-level categories.
    """
        
    CREATE_TABLE = "create table normalized_category_scores " + \
    "select category, category_total, round(category_total/total, 6) as portion " + \
    "from " + \
    "(select substring_index(category,',',1) as category, count(*) as category_total from page_category join traffic_samples on page_category.page_id = traffic_samples.page_id group by 1) as tmp1, " + \
    "(select count(*) as total from traffic_samples) as tmp2;"
    
    DROP_TABLE = 'drop table normalized_category_scores;'

    def __init__(self, **kwargs):
        
        """ Call constructor of parent """
        TableLoader.__init__(self, **kwargs)
        self._table_name_ = 'rfaulk.normalized_category_scores'

    def __del__(self):
        self.close_db()


    def get_all_rows(self):
        """
            Retrieve all rows.

                - Return:
                    - Tuple(tuple).  All rows in the table.
        """
        sql = 'select * from normalized_category_scores'
        return self.execute_SQL(sql)
    

    def get_category_portion(self, category):
        """
            Retrieve the relative frequency of a given category

                - Parameters:
                    - **category**: String.  Top-level category name.

                - Return:
                    - Float.  Relative portion of categorized pages from the table.
        """
        category = MySQLdb._mysql.escape_string(str(category))
        
        sql = "select portion from normalized_category_scores where category = '%s'" % category
        results = self.execute_SQL(sql)
        return float(results[2])
    

    def get_category_count(self, category):
        """
            Retrieve the count of categorized pages.

                - Parameters:
                    - **category**: String.  Top-level category name.

                - Return:
                    - Integer.  Count of categorized pages from the table.

        """

        category = MySQLdb._mysql.escape_string(str(category))
        
        sql = "select category_total from normalized_category_scores where category = '%s'" % category
        results = self.execute_SQL(sql)
        return int(results[1])
    

    def get_record_field(self, row, key):
        """
            Returns the value of a given field indexed on the column from the given row.

                - Parameters:
                    - **row**: Tuple.  A row from the normalized_category_scores table.
                    - **key**: String.  Column name of the field.

                - Return:
                    - Type depends on input.  The value of the field for the given row.
        """
        try:
            if key == 'category':
                return row[0]
            elif key == 'category_total':
                return int(row[1])
            elif key == 'portion':           
                return float(row[2])
        
        except Exception as inst:
            
            logging.error(type(inst))     # the exception instance
            logging.error(inst.args)      # arguments stored in .args
            logging.error(inst)           # __str__ allows args to printed directly
            
            return ''


class e3_pef_iter1_log_data_Loader(TableLoader):
    """
        Subclass of TableLoader that provides custom access to *db42.pmtpa.wmnet.rfaulk.e3_pef_iter1_log_data*.  The definition follows: ::

            +-----------------------------+----------------+------+-----+---------+-------+
            | Field                       | Type           | Null | Key | Default | Extra |
            +-----------------------------+----------------+------+-----+---------+-------+
            | e3pef_project               | varbinary(255) | NO   |     |         |       |
            | e3pef_event                 | varbinary(255) | NO   |     |         |       |
            | e3pef_timestamp             | varbinary(255) | NO   |     |         |       |
            | e3pef_user_category         | varbinary(255) | NO   |     |         |       |
            | e3pef_user_token            | varbinary(255) | NO   |     |         |       |
            | e3pef_namespace             | varbinary(255) | NO   |     |         |       |
            | e3pef_lifetime_edit_count   | varbinary(255) | NO   |     |         |       |
            | e3pef_6month_edit_count     | varbinary(255) | NO   |     |         |       |
            | e3pef_3month_edit_count     | varbinary(255) | NO   |     |         |       |
            | e3pef_last_month_edit_count | varbinary(255) | NO   |     |         |       |
            | e3pef_page_id               | varbinary(255) | NO   |     |         |       |
            | e3pef_rev_id                | varbinary(255) | NO   |     |         |       |
            | e3pef_user_hash             | varbinary(255) | NO   |     |         |       |
            +-----------------------------+----------------+------+-----+---------+-------+

        This table definition is derived from: https://meta.wikimedia.org/wiki/Research:Timestamp_position_modification/Clicktracking
    """

    def __init__(self, **kwargs):
        """ Call constructor of parent """
        TableLoader.__init__(self, db='db42', **kwargs)
        self._table_name_ = 'rfaulk.e3_pef_iter1_log_data'


class e3_last_modified_iter1_log_data_loader(TableLoader):
    """
        Subclass of TableLoader that provides custom access to *db42.pmtpa.wmnet.rfaulk.e3_last_modified_iter1_log_data*.  The definition follows: ::

            +----------------------------+----------------+------+-----+---------+-------+
            | Field                      | Type           | Null | Key | Default | Extra |
            +----------------------------+----------------+------+-----+---------+-------+
            | e3lm_project               | varbinary(255) | NO   |     |         |       |
            | e3lm_event                 | varbinary(255) | NO   |     |         |       |
            | e3lm_timestamp             | varbinary(255) | NO   |     |         |       |
            | e3lm_user_category         | varbinary(255) | NO   |     |         |       |
            | e3lm_user_token            | varbinary(255) | NO   |     |         |       |
            | e3lm_namespace             | varbinary(255) | NO   |     |         |       |
            | e3lm_lifetime_edit_count   | varbinary(255) | NO   |     |         |       |
            | e3lm_6month_edit_count     | varbinary(255) | NO   |     |         |       |
            | e3lm_3month_edit_count     | varbinary(255) | NO   |     |         |       |
            | e3lm_last_month_edit_count | varbinary(255) | NO   |     |         |       |
            | e3lm_additional_data       | varbinary(255) | NO   |     |         |       |
            +----------------------------+----------------+------+-----+---------+-------+

        This table definition is derived from: https://meta.wikimedia.org/wiki/Research:Timestamp_position_modification/Clicktracking
    """

    def __init__(self, **kwargs):
        """ Call constructor of parent """
        TableLoader.__init__(self, db='db42', **kwargs)
        self._table_name_ = 'rfaulk.e3_last_modified_iter1_log_data'