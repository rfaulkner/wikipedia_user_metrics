"""
    Store the query calls for UserMetric classes

    This implements the noop version. It simply returns empty results
    for each call.
"""

__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "february 11th, 2013"
__license__ = "GPL (version 2 or later)"

class UMQueryCallError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Query call failed."):
        Exception.__init__(self, message)

def rev_count_query(uid, is_survival, namespace, project,
                    restrict, start_time, end_time, threshold_ts):
    """ Get count of revisions associated with a UID for Threshold metrics """
    return 0L
rev_count_query.__query_name__ = 'rev_count_query'

def live_account_query(users, project, args):
    """ Format query for live_account metric """
    return []
live_account_query.__query_name__ = 'live_account_query'

def rev_query(users, project, args):
    """ Get revision length, user, and page """
    return []
rev_query.__query_name__ = 'rev_query'

def rev_len_query(rev_id, project):
    """ Get parent revision length - returns long """
    return 0L
rev_len_query.__query_name__ = 'rev_len_query'

def rev_user_query(project, start, end):
    """ Produce all users that made a revision within period """
    return []
rev_user_query.__query_name__ = 'rev_user_query'

def revert_rate_past_revs_query(rev_id, page_id, n, project):
    """ Compute revision history pegged to a given rev """
    return []

def revert_rate_future_revs_query(rev_id, page_id, n, project):
    """ Compute revision future pegged to a given rev """
    return []

def revert_rate_user_revs_query(user, project, args):
    """ Get revision history for a user """
    return []
revert_rate_user_revs_query.__query_name__ = 'revert_rate_user_revs_query'

def time_to_threshold_revs_query(user_id, project, args):
    """ Obtain revisions to perform threshold computation """
    return []
time_to_threshold_revs_query.__query_name__ = 'time_to_threshold_revs_query'

def blocks_user_map_query(users):
    """ Obtain map to generate uname to uid"""
    return {}

def blocks_user_query(users, project, args):
    """ Obtain block/ban events for users """
    return []
blocks_user_query.__query_name__ = 'blocks_user_query'

def edit_count_user_query(users, project, args):
    """  Obtain rev counts by user """
    return []
edit_count_user_query.__query_name__ = 'edit_count_user_query'

def namespace_edits_rev_query(users, project, args):
    """ Obtain revisions by namespace """
    return []
namespace_edits_rev_query.__query_name__ = 'namespace_edits_rev_query'

def user_registration_date(users, project, args):
    return []
user_registration_date.__query_name__ = 'user_registration_date'

query_store = {
    rev_count_query.__query_name__: None,
    live_account_query.__query_name__: None,
    rev_query.__query_name__: None,
    rev_len_query.__query_name__: None,
    rev_user_query.__query_name__: None,
    revert_rate_past_revs_query.__name__: None,
    revert_rate_future_revs_query.__name__: None,
    revert_rate_user_revs_query.__query_name__: None,
    time_to_threshold_revs_query.__query_name__: None,
    blocks_user_map_query.__name__: None,
    blocks_user_query.__query_name__: None,
    edit_count_user_query.__query_name__: None,
    namespace_edits_rev_query.__query_name__: None,
    user_registration_date.__query_name__: None,
    }


