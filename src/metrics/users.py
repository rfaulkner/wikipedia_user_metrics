
"""
    This module handles exposing user types for metrics processing.
"""

__author__  = "ryan faulkner"
__date__    = "01/28/2013"
__email__   = 'rfaulkner@wikimedia.org'

from src.etl.data_loader import Connector

MEDIAWIKI_DB_INSTANCE = 'slave'

class MediaWikiUser(object):
    """
        Class to expose users from MediaWiki databases in a standard way.
        A class level attribute QUERY_TYPES handles the method in which
        the user is extracted from a MediaWiki DB.
    """

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

    def __init__(self):
        super(MediaWikiUser, self).__init__()

    def get_users(self, date_start, date_end, project='enwiki', query_type=1):
        """
            Returns a Generator for MediaWiki user IDs.
        """
        param_dict = {
            'date_start': date_start,
            'date_end': date_end,
            'project' : project,
        }
        conn = Connector(instance=MEDIAWIKI_DB_INSTANCE)
        conn._cur_.execute(self.QUERY_TYPES[query_type] % param_dict)

        for row in conn._cur_: yield row[0]

class MediaWikiUserException(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Error obtaining user(s) from MediaWiki "
                               "instance."):
        Exception.__init__(self, message)
