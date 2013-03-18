
"""
    This module handles exposing user types for metrics processing.

    MediaWikiUser Class
    ~~~~~~~~~~~~~~~~~~~

    This class defines the base operations required to work with MediaWiki
    users.

    User Metric Period
    ~~~~~~~~~~~~~~~~~~

    These classes defines the time periods over which user metrics are
    measured.  ``USER_METRIC_PERIOD_TYPE`` defines the types of ranges over
    which measurements are made:

        * REGISTRATION - the time from user registration until ``t`` hours
        later
        * INPUT - the range defined by ``datetime_start`` and ``datetime_end``

    ``USER_METRIC_PERIOD_DATA`` is a simple carrier for the tuples that define
    each users range.  Finally, the ``UserMetricPeriod`` themselves define a
    ``get`` method which returns ``USER_METRIC_PERIOD_DATA`` objects containing
    the ranges for each user.
"""

__author__ = "ryan faulkner"
__date__ = "01/28/2013"
__email__ = 'rfaulkner@wikimedia.org'

from user_metrics.config import logging, settings

from user_metrics.etl.data_loader import Connector
from datetime import datetime, timedelta
from user_metrics.metrics import query_mod
from collections import namedtuple
from user_metrics.utils import enum, format_mediawiki_timestamp
from dateutil.parser import parse as date_parse

# Module level query definitions
# @TODO move these to the query package


SELECT_PROJECT_IDS =\
    """
        SELECT
            rev_user,
            COUNT(*) as revs
        FROM
            %(project)s.revision
        WHERE
            rev_user IN (
                SELECT user_id
                FROM %(project)s.user
                WHERE user_registration > '%(ts_start)s'
                    AND user_registration < '%(ts_end_user)s')
            AND rev_timestamp > '%(ts_start)s'
            AND rev_timestamp <= '%(ts_end_revs)s'
        GROUP BY 1
        HAVING revs > %(rev_lower_limit)s
        ORDER BY 2 DESC
        LIMIT %(max_size)s;
    """



# Cohort Processing Methods
# =========================


def generate_test_cohort_name(project):
    """
        Generates a name for a test cohort to be inserted into usertags[_meta]
    """
    return 'testcohort_{0}_{1}'.\
        format(project,
               format_mediawiki_timestamp(datetime.now()))


def generate_test_cohort(project,
                         max_size=10,
                         write=False,
                         user_interval_size=1,
                         rev_interval_size=7,
                         rev_lower_limit=0):
    """
        Build a test cohort (list of UIDs) for the given project.

        Parameters
        ~~~~~~~~~~

        project : str
           Wikipedia project e.g. 'enwiki'.

        size : uint
           Number of users to include in the cohort.

        write: boolean
           Flag indicating whether to write the cohort to
           settings.__cohort_meta_db__ and settings.__cohort_db__.

        user_interval_size: uint
            Number of days within which to take registered users

        rev_lower_limit: int
            Minimum number of revisions a user must have between registration
            and the

        Returns the list of UIDs from the corresponding project that defines
        the test cohort.
    """

    # Determine the time bounds that define the cohort acceptance criteria

    ts_start_o = datetime.now() + timedelta(days=-60)
    ts_end_user_o = ts_start_o + timedelta(days=int(user_interval_size))
    ts_end_revs_o = ts_start_o + timedelta(days=int(rev_interval_size))

    ts_start = format_mediawiki_timestamp(ts_start_o)
    ts_end_user = format_mediawiki_timestamp(ts_end_user_o)
    ts_end_revs = format_mediawiki_timestamp(ts_end_revs_o)

    # Synthesize query and execute
    logging.info(__name__ + ' :: Getting users from {0}.\n\n'
                            '\tUser interval: {1} - {2}\n'
                            '\tRevision interval: {1} - {3}\n'
                            '\tMax users = {4}\n'
                            '\tMin revs = {5}\n'.
                            format(project,
                                   ts_start,
                                   ts_end_user,
                                   ts_end_revs,
                                   max_size,
                                   rev_lower_limit
                                   )
                 )
    select_users = SELECT_PROJECT_IDS % {
        'project': project,
        'ts_start': ts_start,
        'ts_end_user': ts_end_user,
        'ts_end_revs': ts_end_revs,
        'max_size': max_size,
        'rev_lower_limit': rev_lower_limit,
    }
    conn = Connector(instance=settings.PROJECT_DB_MAP[project])
    conn._cur_.execute(select_users)
    users = [row for row in conn._cur_]
    del conn

    # get latest cohort id & cohort name
    utm_name = generate_test_cohort_name(project)

    # add new ids to usertags & usertags_meta
    if write:
        logging.info(__name__ + ' :: Inserting records...\n\n'
                                '\tCohort name - {0}\n'
                                '\t{2} - {3} record(s)\n'.
                                format(utm_name,
                                       settings.__cohort_db__,
                                       len(users)))
        query_mod.add_cohort_data(utm_name, users, project)

    return users


# User Classes
# ============

class MediaWikiUserException(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Error obtaining user(s) from MediaWiki "
                               "instance."):
        Exception.__init__(self, message)


class MediaWikiUser(object):
    """
        Class to expose users from MediaWiki databases in a standard way.
        A class level attribute QUERY_TYPES handles the method in which
        the user is extracted from a MediaWiki DB.
    """

    # @TODO move these to the query package
    # Queries MediaWiki database for account creations via Logging table
    USER_QUERY_LOG = """
                    SELECT log_user
                    FROM %(project)s.logging
                    WHERE log_timestamp > %(date_start)s AND
                     log_timestamp <= %(date_end)s AND
                     log_action = 'create' AND log_type='newusers'
                """

    # Queries MediaWiki database for account creations via User table
    USER_QUERY_USER = """
                    SELECT user_id
                    FROM %(project)s.user
                    WHERE user_registration > %(date_start)s AND
                     user_registration <= %(date_end)s
                """

    QUERY_TYPES = {
        1: USER_QUERY_LOG,
        2: USER_QUERY_USER,
    }

    def __init__(self, query_type=1):
        self._query_type = query_type
        super(MediaWikiUser, self).__init__()

    def get_users(self, date_start, date_end, project='enwiki'):
        """
            Returns a Generator for MediaWiki user IDs.
        """
        param_dict = {
            'date_start': format_mediawiki_timestamp(date_start),
            'date_end': format_mediawiki_timestamp(date_end),
            'project': project,
        }
        conn = Connector(instance=settings.__cohort_data_instance__)
        conn._cur_.execute(self.QUERY_TYPES[self._query_type] % param_dict)

        for row in conn._cur_:
            yield row[0]

    def map_user_id(self, users, project_in, project_out):
        """
            Map user IDs between projects.  Requires access to centralauth
            database.
        """
        raise NotImplementedError()


# Define User Metric Periods
# ==========================

# enumeration for user periods
USER_METRIC_PERIOD_TYPE = enum('REGISTRATION', 'INPUT', 'REGINPUT')
USER_METRIC_PERIOD_DATA = namedtuple('UMPData', 'user start end')


class UserMetricPeriod(object):
    """
        Base class of family.  Sub-classes define 1) the ``start`` and ``end``
        attributes of the ``USER_METRIC_PERIOD_DATA`` type 2) any conditions
        on the returned users for a given time-period. ::

            >>> UserMetricPeriod().get(['123456','234567', ...], BytesAdded())
    """
    @staticmethod
    def get(users, metric):
        """
            Returns a list of users and ranges in ``USER_METRIC_PERIOD_DATA``
            objects.

            Parameters
            ~~~~~~~~~~

                users : list
                    List of user IDs.

                metric : UserMetric
                    Metric object or interface exposing timestamp data.
        """
        raise NotImplementedError()


class UMPRegistration(UserMetricPeriod):
    """
        This ``UserMetricPeriod`` class returns the set of users with
        ``start`` and ``end`` time defined by the user registration
        date and ``t`` hours later.
    """
    @staticmethod
    def get(users, metric):
        for row in query_mod.user_registration_date(users, metric.project,
                                                    None):
            reg = date_parse(row[1])
            end = reg + timedelta(hours=int(metric.t))
            yield USER_METRIC_PERIOD_DATA(row[0],
                                          format_mediawiki_timestamp(reg),
                                          format_mediawiki_timestamp(end))


class UMPInput(UserMetricPeriod):
    """
        This ``UserMetricPeriod`` class returns the set of users
        with the ``start`` and ``end`` timestamps defined by ``metric``.
    """
    @staticmethod
    def get(users, metric):
        for user in users:
            yield USER_METRIC_PERIOD_DATA(user,
                                          format_mediawiki_timestamp
                                          (metric.datetime_start),
                                          format_mediawiki_timestamp
                                          (metric.datetime_end))


class UMPRegInput(UserMetricPeriod):
        """
            This ``UserMetricPeriod`` class returns the set of users
            conditional on their registration falling within the time interval
            defined by ``metric``.
        """
        @staticmethod
        def get(users, metric):
            for row in query_mod.user_registration_date(users, metric.project,
                                                        None):

                user = row[0]
                reg = date_parse(row[1])

                start = format_mediawiki_timestamp(metric.datetime_start)
                end = format_mediawiki_timestamp(metric.datetime_end)

                if date_parse(start) <= reg <= date_parse(end):
                    reg_plus_t = reg + timedelta(hours=int(metric.t))
                    yield USER_METRIC_PERIOD_DATA(user,
                                                  format_mediawiki_timestamp
                                                  (reg),
                                                  format_mediawiki_timestamp
                                                  (reg_plus_t))
                else:
                    continue


# Define a mapping from UMP types to get methods
UMP_MAP = {
    USER_METRIC_PERIOD_TYPE.REGISTRATION: UMPRegistration.get,
    USER_METRIC_PERIOD_TYPE.INPUT: UMPInput.get,
    USER_METRIC_PERIOD_TYPE.REGINPUT: UMPRegInput.get,
}


if __name__ == '__main__':
    generate_test_cohort('dewiki', write=True)