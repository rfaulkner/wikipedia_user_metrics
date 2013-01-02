"""
    Metrics API module: http://metrics-api.wikimedia.org/

    Defines the API which exposes metrics on Wikipedia users.  The metrics are defined at
    https://meta.wikimedia.org/wiki/Research:Metrics.
"""

COHORT_OP_AND = '&'
COHORT_OP_OR = '|'
COHORT_OP_NOT = '^'

from src.etl.data_loader import Connector

def parse_cohorts(expression):
    """
        Defines and parses boolean expressions of cohorts and returns a list of user ids corresponding to the expression
         argument.

            Parameters:
                - **expression**: str. Boolean expression built of cohort labels.

            Return:
                - List(str).  user ids corresponding to cohort expression.
    """
    conn = Connector(instance='slave')

    # parse expression

    # retrieve user ids from data store

    del conn