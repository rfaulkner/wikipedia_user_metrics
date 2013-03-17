
"""
    The engine for the metrics API which stores definitions an backend API
    operations.  This module defines the communication between API requests
    and UserMetric objects, how and where request responses are stored, and
    how cohorts are parsed from API request URLs.



    Cohort request parsing
    ~~~~~~~~~~~~~~~~~~~~~~

    This set of methods allows boolean expressions of cohort IDs to be
    synthesized and interpreted in the portion of the URL path that is
    bound to the user cohort name.  This set of methods, invoked at the top
    level via ``parse_cohorts`` takes an expression of the form::

        http://metrics-api.wikimedia.org/cohorts/1&2~3~4/bytes_added

    The portion of the path ``1&2~3~4``, resolves to the boolean expression
    "1 AND 2 OR 3 OR 4".  The cohorts that correspond to the numeric ID values
    in ``usertags_meta`` are resolved to sets of user ID longs which are then
    operated on with union and intersect operations to yield a custom user
    list.  The power of this functionality lies in that it allows subsets of
    users to be selected based on prior conditions that includes them in a
    given cohort.

    Method Definitions
    ~~~~~~~~~~~~~~~~~~
"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "january 11 2012"
__license__ = "GPL (version 2 or later)"

from re import search
import user_metrics.etl.data_loader as dl
from user_metrics.api import MetricsAPIError
from user_metrics.utils import enum


#
# Define constants for request_manager module
# ===========================================

# Define Process status types
JOB_STATUS_TYPES = ['pending', 'running', 'success', 'failure']
JOB_STATUS = eval('enum("' + '","'.join(JOB_STATUS_TYPES) +
                  '", **' + str({t[0]: t[1] for t in zip(JOB_STATUS_TYPES,
                  JOB_STATUS_TYPES)}) + ')')

#
# Define remaining constants
# ==========================
# @TODO break these out into separate modules

# Regex that matches a MediaWiki user ID
MW_UID_REGEX = r'^[0-9]{5}[0-9]*$'
MW_UNAME_REGEX = r'[a-zA-Z_\.\+ ]'

# Datetime string format to be used throughout the API
DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"

# The default value for non-assigned and valid values in the query string
DEFAULT_QUERY_VAL = 'present'


#
# Cohort parsing methods
#
# ======================

# This regex must be matched to parse cohorts
COHORT_REGEX = r'^([0-9]+[&~])*[0-9]+$'

COHORT_OP_AND = '&'
COHORT_OP_OR = '~'
# COHORT_OP_NOT = '^'


def parse_cohorts(expression):
    """
        Defines and parses boolean expressions of cohorts and returns a list
        of user ids corresponding to the expression argument.

            Parameters:
                - **expression**: str. Boolean expression built of
                    cohort labels.

            Return:
                - List(str).  user ids corresponding to cohort expression.
    """

    # match expression
    if not search(COHORT_REGEX, expression):
        raise MetricsAPIError()

    # parse expression
    return parse(expression)


def parse(expression):
    """ Top level parsing. Splits expression by OR then sub-expressions by
        AND. returns a generator of ids included in the evaluated expression
    """
    user_ids_seen = set()
    for sub_exp_1 in expression.split(COHORT_OP_OR):
        for user_id in intersect_ids(sub_exp_1.split(COHORT_OP_AND)):
            if not user_ids_seen.__contains__(user_id):
                user_ids_seen.add(user_id)
                yield user_id


def get_cohort_ids(conn, cohort_id):
    """ Returns string valued ids corresponding to a cohort """
    sql = """
        SELECT ut_user
        FROM staging.usertags
        WHERE ut_tag = %(id)s
    """ % {
        'id': str(cohort_id)
    }
    conn._cur_.execute(sql)
    for row in conn._cur_:
        yield str(row[0])


def intersect_ids(cohort_id_list):

    conn = dl.Connector(instance='slave')

    user_ids = dict()
    # only a single cohort id in the expression - return all users of this
    # cohort
    if len(cohort_id_list) == 1:
        for id in get_cohort_ids(conn, cohort_id_list[0]):
            yield id
    else:
        for cid in cohort_id_list:
            for id in get_cohort_ids(conn, cid):
                if id in user_ids:
                    user_ids[id] += 1
                else:
                    user_ids[id] = 1
                    # Executes only in the case that there was more than one
                    # cohort id in the expression
        for key in user_ids:
            if user_ids[key] > 1:
                yield key
    del conn
