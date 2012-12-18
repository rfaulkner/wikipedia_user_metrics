
import sys
import MySQLdb
import logging
import table_loader as tl
import normalized_category_scores_table_loader as ncst

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class PageCategoryTableLoader(tl.TableLoader):
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


        tl.TableLoader.__init__(self, **kwargs)
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
                    Dictionary(String).  **key**: Top-level categories name; **value**: count of occurrence as most relevant category among page IDs
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

        norm_results = ncst.NormalizedCategoryScoresTableLoader().get_all_rows()
        norm_cats = dict()

        for row in norm_results:
            category = ncst.NormalizedCategoryScoresTableLoader().get_record_field(row, 'category')
            portion = ncst.NormalizedCategoryScoresTableLoader().get_record_field(row, 'portion')
            norm_cats[category] = portion

        category_counts = self.get_article_categories_by_page_ids(page_id_list)
        cat_count_total = 0.0

        for category in category_counts:
            cat_count_total = cat_count_total + category_counts[category]
        for category in category_counts:
            category_counts[category] = float(category_counts[category]) / cat_count_total

        category_score = dict()
        for category in norm_cats:
            try:
                category_score[category] = (category_counts[category] - norm_cats[category]) / norm_cats[category] * 100.0
            except Exception:
                category_score[category] = -1.0
                pass

        return category_score