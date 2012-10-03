
__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2011"

import sys
import MySQLdb
import logging
import config.settings as projSet
import TableLoader as TL

sys.path.append(projSet.__wsor_msg_templates_home_dir__)
import umetrics.postings as post


# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class NormalizedCategoryScoresTableLoader(TL.TableLoader):
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

    CREATE_TABLE = "create table normalized_category_scores " +\
                   "select category, category_total, round(category_total/total, 6) as portion " +\
                   "from " +\
                   "(select substring_index(category,',',1) as category, count(*) as category_total from page_category join traffic_samples on page_category.page_id = traffic_samples.page_id group by 1) as tmp1, " +\
                   "(select count(*) as total from traffic_samples) as tmp2;"

    DROP_TABLE = 'drop table normalized_category_scores;'

    def __init__(self, **kwargs):

        """ Call constructor of parent """
        TL.TableLoader.__init__(self, **kwargs)
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

