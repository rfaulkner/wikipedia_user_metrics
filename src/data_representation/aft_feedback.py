
"""
    Format data representation for Wikipedia feedback solicited via the article feedback tool (AFT).

    The results of the AFT feedback is logged in three tables described below.

    s1-analytics-slave.eqiad.wmnet.enwiki.aft_article_feedback: ::
        +--------------------------+------------------+------+-----+----------------+----------------+
        | Field                    | Type             | Null | Key | Default        | Extra          |
        +--------------------------+------------------+------+-----+----------------+----------------+
        | af_id                    | int(10) unsigned | NO   | PRI | NULL           | auto_increment |
        | af_page_id               | int(10) unsigned | NO   | MUL | NULL           |                |
        | af_user_id               | int(11)          | NO   | MUL | NULL           |                |
        | af_user_ip               | varbinary(32)    | YES  |     | NULL           |                |
        | af_user_anon_token       | varbinary(32)    | NO   |     |                |                |
        | af_revision_id           | int(10) unsigned | NO   | MUL | NULL           |                |
        | af_bucket_id             | int(10) unsigned | NO   |     | 0              |                |
        | af_cta_id                | int(10) unsigned | NO   |     | 0              |                |
        | af_link_id               | int(10) unsigned | NO   |     | 0              |                |
        | af_created               | binary(14)       | NO   | MUL |                |                |
        | af_abuse_count           | int(10) unsigned | NO   |     | 0              |                |
        | af_helpful_count         | int(10) unsigned | NO   |     | 0              |                |
        | af_unhelpful_count       | int(10) unsigned | NO   |     | 0              |                |
        | af_oversight_count       | int(10) unsigned | NO   |     | 0              |                |
        | af_is_deleted            | tinyint(1)       | NO   |     | 0              |                |
        | af_is_hidden             | tinyint(1)       | NO   |     | 0              |                |
        | af_net_helpfulness       | int(11)          | NO   |     | 0              |                |
        | af_has_comment           | tinyint(1)       | NO   |     | 0              |                |
        | af_is_unhidden           | tinyint(1)       | NO   |     | 0              |                |
        | af_is_undeleted          | tinyint(1)       | NO   |     | 0              |                |
        | af_is_declined           | tinyint(1)       | NO   |     | 0              |                |
        | af_activity_count        | int(10) unsigned | NO   |     | 0              |                |
        | af_form_id               | int(10) unsigned | NO   |     | 0              |                |
        | af_experiment            | varbinary(32)    | YES  | MUL | NULL           |                |
        | af_suppress_count        | int(10) unsigned | NO   |     | 0              |                |
        | af_last_status           | varbinary(16)    | YES  |     | NULL           |                |
        | af_last_status_user_id   | int(10) unsigned | NO   |     | 0              |                |
        | af_last_status_timestamp | binary(14)       | YES  |     |                |                |
        | af_is_autohide           | tinyint(1)       | NO   |     | 0              |                |
        | af_is_unrequested        | tinyint(1)       | NO   |     | 0              |                |
        | af_is_featured           | tinyint(1)       | NO   |     | 0              |                |
        | af_is_unfeatured         | tinyint(1)       | NO   |     | 0              |                |
        | af_is_resolved           | tinyint(1)       | NO   |     | 0              |                |
        | af_is_unresolved         | tinyint(1)       | NO   |     | 0              |                |
        | af_relevance_score       | int(11)          | NO   |     | 0              |                |
        | af_relevance_sort        | int(11)          | NO   | MUL | 0              |                |
        | af_last_status_notes     | varbinary(255)   | YES  |     | NULL           |                |
        +--------------------------+------------------+------+-----+----------------+----------------+

    s1-analytics-slave.eqiad.wmnet.enwiki.aft_article_answer: ::
        +-----------------------+------------------+------+-----+---------+-------+
        | Field                 | Type             | Null | Key | Default | Extra |
        +-----------------------+------------------+------+-----+---------+-------+
        | aa_feedback_id        | int(10) unsigned | NO   | PRI | NULL    |       |
        | aa_field_id           | int(10) unsigned | NO   | PRI | NULL    |       |
        | aa_response_rating    | int(11)          | YES  |     | NULL    |       |
        | aa_response_text      | varbinary(255)   | YES  |     | NULL    |       |
        | aat_id                | int(10) unsigned | YES  |     | NULL    |       |
        | aa_response_boolean   | tinyint(1)       | YES  |     | NULL    |       |
        | aa_response_option_id | int(10) unsigned | YES  |     | NULL    |       |
        +-----------------------+------------------+------+-----+---------+-------+

    s1-analytics-slave.eqiad.wmnet.enwiki.aft_article_answer_text: ::
        +-------------------+------------------+------+-----+---------+----------------+
        | Field             | Type             | Null | Key | Default | Extra          |
        +-------------------+------------------+------+-----+---------+----------------+
        | aat_id            | int(10) unsigned | NO   | PRI | NULL    | auto_increment |
        | aat_response_text | blob             | NO   |     | NULL    |                |
        +-------------------+------------------+------+-----+---------+----------------+

    s1-analytics-slave.eqiad.wmnet.enwiki.logging: ::
        +---------------+---------------------+------+-----+----------------+----------------+
        | Field         | Type                | Null | Key | Default        | Extra          |
        +---------------+---------------------+------+-----+----------------+----------------+
        | log_id        | int(10) unsigned    | NO   | PRI | NULL           | auto_increment |
        | log_type      | varbinary(32)       | NO   | MUL |                |                |
        | log_action    | varbinary(32)       | NO   | MUL |                |                |
        | log_timestamp | varbinary(14)       | NO   | MUL | 19700101000000 |                |
        | log_user      | int(10) unsigned    | NO   | MUL | 0              |                |
        | log_namespace | int(11)             | NO   | MUL | 0              |                |
        | log_title     | varbinary(255)      | NO   | MUL |                |                |
        | log_comment   | varbinary(255)      | NO   |     |                |                |
        | log_params    | blob                | NO   |     | NULL           |                |
        | log_deleted   | tinyint(3) unsigned | NO   |     | 0              |                |
        | log_user_text | varbinary(255)      | NO   |     |                |                |
        | log_page      | int(10) unsigned    | YES  | MUL | NULL           |                |
        +---------------+---------------------+------+-----+----------------+----------------+


    With each feedback event is associated a answer text

    FEATURES:

        * answer feedback length
        * is featured (af_is_featured)
        * is hidden (af_is_hidden)
        * is unhidden (af_is_unhidden)
        * times tagged as helpful (af_helpful_count)

    The data model definition of the samples is the following: ::

        * is_featured       - is the feedback feature
        * is_hidden         - is the feedback hidden
        * is_unhidden       - is the feedback unhidden
        * is_autohide       - has autohide been activiated?
        * is_resolved       -
        * helpful_count     - how many times has the feedback been flagged as helpful
        * response_boolean  -
        * abuse_count       - how many times has the feedback been flagged as helpful
        * feedback_length   - length in bytes of the feedback comment
        * post_mod_count    - number of moderations on the feedback

    EXAMPLES: ::

        >>> import src.data_modelling.aft_feedback as aft
        >>> f = aft.AFTFeedbackFactory().__iter__()
        >>> d = f.next()
        2012-11-27 14:18:50.191411 - SFT Feedback - start = "2012-11-26 14:18:50.191386", end = "2012-11-27 14:18:50.191386"
        >>> d
        (0, AFT_feedback(is_hidden=0, is_unhidden=0, is_autohide=0, is_resolved=0, helpful_count=1L, response_boolean=None, abuse_count=0L, feedback_length=308L, post_mod_count=1))
        >>> for d in f: print d
        ...
        (0, AFT_feedback(is_hidden=1, is_unhidden=0, is_autohide=0, is_resolved=0, helpful_count=0L, response_boolean=None, abuse_count=0L, feedback_length=379L, post_mod_count=1))
        (0, AFT_feedback(is_hidden=0, is_unhidden=0, is_autohide=0, is_resolved=1, helpful_count=0L, response_boolean=None, abuse_count=0L, feedback_length=278L, post_mod_count=1))
        (0, AFT_feedback(is_hidden=1, is_unhidden=0, is_autohide=0, is_resolved=0, helpful_count=0L, response_boolean=None, abuse_count=0L, feedback_length=1436L, post_mod_count=1))
        ...
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "November 27th, 2012"
__license__ = "GPL (version 2 or later)"

import collections
import datetime
import src.etl.data_loader as dl
import src.metrics.user_metric as um

TBL_AFT_FEEDBACK = "aft_article_feedback"
TBL_ANSWER = "aft_article_answer"
TBL_ANSWER_TEXT = "aft_article_answer_text"
TBL_logging = "logging"


class AFTFeedbackFactory(object):

    __target_feature_idx = 1 # Marks the feature which is the supervised learning signal
    __instance = None

    def __init__(self, *args, **kwargs):

        self.__feature_list = ['is_hidden', 'is_unhidden', 'is_autohide', 'is_resolved',
                               'helpful_count', 'response_boolean', 'abuse_count', 'feedback_length', 'post_mod_count']
        self.__feature_types = [bool, bool, bool, int, long]
        self.__tuple_cls = collections.namedtuple("AFT_feedback", " ".join(self.__feature_list))

        super(AFTFeedbackFactory, self).__init__()

    def __new__(cls, *args, **kwargs):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(AFTFeedbackFactory, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def __iter__(self, **kwargs):

        conn = dl.Connector(instance='slave')
        is_gen, start_date, end_date =  self.process_kwargs(**kwargs)

        sql =\
        """
            select
                af_id,
                af_is_featured,
                af_is_hidden,
                af_is_unhidden,
                af_is_autohide,
                af_is_resolved,
                af_helpful_count,
                ifnull(aa_response_boolean,0),
                af_abuse_count,
                length(aat_response_text) as text_len
            from enwiki.%(feedback_table)s as af
                join enwiki.%(answer_table)s as aa
                on af.af_id = aa.aa_feedback_id
                join enwiki.%(answer_text_table)s as aat
                on aa.aat_id = aat.aat_id
            where af_created > "%(start)s" and af_created < "%(end)s"
        """ % {
            'start' : start_date,
            'end' : end_date,
            'feedback_table' : TBL_AFT_FEEDBACK,
            'answer_table' : TBL_ANSWER,
            'answer_text_table' : TBL_ANSWER_TEXT
        }


        moderator_dict = self._get_logging_events(**kwargs)

        # compose feature vectors
        conn._cur_.execute(" ".join(sql.strip().split('\n')))
        if is_gen:
            for r in conn._cur_:
                try:
                    if moderator_dict.has_key(long(r[0])):
                        yield (r[AFTFeedbackFactory.__target_feature_idx],
                               self.__tuple_cls(r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],moderator_dict[long(r[0])]))
                    else:
                        yield (r[AFTFeedbackFactory.__target_feature_idx],
                               self.__tuple_cls(r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],0))
                except IndexError:
                    continue
                except TypeError:
                    continue

        del conn

    def __doc__(self): raise NotImplementedError

    @property
    def fields(self): return self.__feature_list
    @property
    def field_types(self): return self.__feature_list

    def process_kwargs(self, **kwargs):

        td = datetime.timedelta(days=-1)
        now = datetime.datetime.now()

        is_gen = bool(kwargs['is_gen']) if 'is_gen' in kwargs else True
        start_date = str(kwargs['start_date']) if 'start_date' in kwargs else str((now + td))
        end_date = str(kwargs['end_date']) if 'end_date' in kwargs else str(now)

        print str(datetime.datetime.now()) + ' - AFT Feedback - start = "%s", end = "%s" ' % (start_date, end_date)

        return is_gen, um.UserMetric._get_timestamp(start_date), um.UserMetric._get_timestamp(end_date)


    def _get_logging_events(self, **kwargs):
        """ pull moderated feedback from the logging table separately """

        conn = dl.Connector(instance='slave')
        is_gen, start_date, end_date =  self.process_kwargs(**kwargs)

        # Note for this query to function it is dependent on the form of `log_params` in the logging table
        # e.g. a:3:{s:6:"source";s:7:"article";s:10:"feedbackId";i:684703;s:6:"pageId";i:17689593;}
        sql = \
        """
            select
                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(log_params, ';', 4),':',-1) as UNSIGNED) as feedback_id,
                count(*) as moderations
             from enwiki.%(logging_table)s
             where log_type = 'articlefeedbackv5' and log_timestamp > "%(start)s" and log_timestamp <= "%(end)s"
             group by 1
             having feedback_id != 0
        """ % {
            'start' : start_date,
            'end' : end_date,
            'logging_table' : TBL_logging
        }

        conn._cur_.execute(" ".join(sql.strip().split('\n')))
        moderator_dict = dict()
        for r in conn._cur_:
            try:
                moderator_dict[long(r[0])] = int(r[1])
            except KeyError:
                continue
            except IndexError:
                continue
        del conn
        return moderator_dict

# Testing
if __name__ == "__main__":
    f = AFTFeedbackFactory().__iter__()
    l=list()
    for i in f: l.append(i)

    # AFTFeedbackFactory()._get_logging_events()