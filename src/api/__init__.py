"""
    Metrics API module: http://metrics-api.wikimedia.org/

    Defines the API which exposes metrics on Wikipedia users.  The metrics are defined at
    https://meta.wikimedia.org/wiki/Research:Metrics.
"""

COHORT_REGEX = r'^([0-9]+[&|~])*[0-9]+$' # This regex must be matched to parse cohorts
COHORT_OP_AND = '&'
COHORT_OP_OR = '~'
# COHORT_OP_NOT = '^'

from src.etl.data_loader import Connector
from re import search

def parse_cohorts(expression):
    """
        Defines and parses boolean expressions of cohorts and returns a list of user ids corresponding to the expression
         argument.

            Parameters:
                - **expression**: str. Boolean expression built of cohort labels.

            Return:
                - List(str).  user ids corresponding to cohort expression.
    """

    # match expression
    if not search(COHORT_REGEX, expression):
        raise MetricsAPIError()

    # parse expression
    yield parse(expression)



def parse(expression):
    """ Top level parsing. Splits expression by OR then sub-expressions by AND. returns a generator of ids
     included in the evaluated expression """
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
        'id' : str(cohort_id)
    }
    conn._cur_.execute(sql)
    for row in conn._cur_:
        yield str(row[0])

def intersect_ids(cohort_id_list):

    conn = Connector(instance='slave')

    user_ids = dict()
    if len(cohort_id_list) == 1: # only a single cohort id in the expression - return all users of this cohort
        for id in get_cohort_ids(conn, cohort_id_list[0]):
            yield id
    else:
        for cid in cohort_id_list:
            for id in get_cohort_ids(conn, cid):
                if user_ids.has_key(id):
                    user_ids[id] += 1
                else:
                    user_ids[id] = 1
        # Executes only in the case that there was more than one cohort id in the expression
        for key in user_ids:
            if user_ids[key] > 1: yield key
    del conn

class MetricsAPIError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Error processing API request."):
        Exception.__init__(self, message)