
"""
    Store the query calls for UserMetric classes
"""

__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "january 30th, 2013"
__license__ = "GPL (version 2 or later)"

from src.etl.data_loader import DataLoader
from MySQLdb import escape_string
from copy import deepcopy

def escape_var(var):
    """
        Escapes either elements of a list (recursively visiting elements)
        or a single variable.  The variable is cast to string before being
        escaped.

        - Parameters:
            - **var**: List or string.  Variable or list (potentially
                nested) of variables to be escaped.

        - Return:
            - List or string.  escaped elements.
    """

    # If the input is a list recursively call on elements
    if hasattr(var, '__iter__'):
        escaped_var = list()
        for elem in var: escaped_var.append(escape_var(elem))
        return escaped_var
    else:
        return escape_string(str(var))

def format_namespace(namespace):
    """ Format the namespace condition in queries and returns the string.

        Expects a list of numeric namespace keys.  Otherwise returns
        an empty condition string
    """
    ns_cond = ''

    if hasattr(namespace, '__iter__'):
        if len(namespace) == 1:
            ns_cond = 'page_namespace = ' + str(namespace.pop())
        else:
            ns_cond = 'page_namespace in (' + \
                      ",".join(DataLoader().
                                cast_elems_to_string(list(namespace))) + ')'
    return ns_cond

def threshold_reg_query(users, project):
    """ Get registered users for Threshold metric objects """
    uid_str = DataLoader().\
    format_comma_separated_list(
        DataLoader().
        cast_elems_to_string(users),
        include_quotes=False)

    # Get all registrations - this assumes that each user id corresponds
    # to a valid registration event in the the logging table.
    sql = query_store[threshold_reg_query.__name__] % {
        'project' : project,
        'uid_str' : uid_str
    }
    return " ".join(sql.strip().split('\n'))

def threshold_rev_query(uid, is_survival, namespace, project,
                        restrict, start_time, end_time, threshold_ts):
    """ Get revisions associated with a UID for Threshold metrics """

    # The key difference between survival and threshold is that threshold
    # measures a level of activity before a point whereas survival
    # (generally) measures any activity after a point
    if is_survival:
        timestamp_cond = ' and rev_timestamp > %(ts)s'
    else:
        timestamp_cond = ' and rev_timestamp <= "%(ts)s"'

    # format the namespace condition
    ns_cond = format_namespace(deepcopy(namespace))

    # Format condition on timestamps
    if restrict:
        timestamp_cond += ' and rev_timestamp > "{0}" and '\
                          'rev_timestamp <= "{1}"'.format(start_time,
                                                        end_time)

    sql = query_store[threshold_rev_query.__name__] + timestamp_cond

    sql = sql % {'project' : project,
                'ts' : threshold_ts,
                'ns' : ns_cond,
                'uid' : uid}
    return " ".join(sql.strip().split('\n'))

def live_account_query(users, namespace, project):
    """ Format query for live_account metric """

    user_cond = DataLoader().format_condition_in('ept_user', users)
    ns_cond = format_namespace(namespace)

    where_clause = 'log_action = "create"'
    if user_cond: where_clause += ' AND ' + user_cond
    if ns_cond: where_clause += ' AND ' + ns_cond

    from_clause = '%(project)s.edit_page_tracking AS e RIGHT JOIN ' \
                  '%(project)s.logging AS l ON e.ept_user = l.log_user'
    from_clause = from_clause % {"project" : project}
    if ns_cond:
        from_clause += " LEFT JOIN %(project)s.page as p " \
                        "ON e.ept_title = p.page_title" % \
                       { "project" : project}

    sql = query_store[live_account_query.__name__] % \
                {
                    "from_clause" : from_clause,
                    "where_clause" : where_clause,
                }
    return " ".join(sql.split('\n'))

def bytes_added_rev_query(start, end, users, namespace, project):
    """ Get revision length, user, and page """
    ts_condition  = 'rev_timestamp >= "%s" and rev_timestamp < "%s"' % \
                                                                (start, end)

    # build the user set for inclusion into the query - if the user_handle is
    # empty or None get all users for timeframe

    # 1. Escape user_handle for SQL injection
    # 2. Ensure the handles are iterable
    users = escape_var(users)
    if not hasattr(users, '__iter__'): users = [users]

    user_set = DataLoader().format_comma_separated_list(users,
        include_quotes=False)
    where_clause = 'rev_user in (%(user_set)s) and %(ts_condition)s' % {
        'user_set' : user_set, 'ts_condition' : ts_condition}

    # format the namespace condition
    ns_cond = format_namespace(namespace)
    if ns_cond: ns_cond += ' and'

    sql = query_store[bytes_added_rev_query.__name__] % {
        'where_clause' : where_clause,
        'project' : project,
        'namespace' : ns_cond}
    return " ".join(sql.split('\n'))

def bytes_added_rev_len_query(rev_id, project):
    """ Get parent revision length """
    return query_store[bytes_added_rev_len_query.__name__] % {
        'project' : project,
        'parent_rev_id' : rev_id,
    }

def bytes_added_rev_user_query(start, end):
    """ Produce all users that made a revision within period """
    return query_store[bytes_added_rev_user_query.__name__] % {
        'start': start,
        'end': end,
    }

query_store = {
    threshold_reg_query.__name__:
                            """
                                SELECT
                                    log_user,
                                    log_timestamp
                                FROM %(project)s.logging
                                WHERE log_action = 'create' AND
                                    log_type='newusers'
                                        and log_user in (%(uid_str)s)
                            """,

    threshold_rev_query.__name__:
                            """
                                SELECT
                                    count(*) as revs
                                FROM %(project)s.revision as r
                                    JOIN %(project)s.page as p
                                        ON  r.rev_page = p.page_id
                                WHERE %(ns)s AND rev_user = %(uid)s
                            """,
    live_account_query.__name__:
                            """
                                SELECT
                                    e.ept_user,
                                    MIN(l.log_timestamp) as registration,
                                    MIN(e.ept_timestamp) as first_click
                                FROM %(from_clause)s
                                WHERE %(where_clause)s
                                GROUP BY 1
                            """,
    bytes_added_rev_query.__name__:
                            """
                                select
                                    rev_user,
                                    rev_len,
                                    rev_parent_id
                                from %(project)s.revision
                                    join %(project)s.page
                                    on page.page_id = revision.rev_page
                                where %(namespace)s %(where_clause)s
                            """,
    bytes_added_rev_len_query.__name__:
                            """
                                SELECT rev_len
                                FROM %(project)s.revision
                                WHERE rev_id = %(parent_rev_id)s
                            """,
    bytes_added_rev_user_query.__name__:
                            """
                                SELECT distinct rev_user
                                FROM enwiki.revision
                                WHERE rev_timestamp >= "%(start)s" AND
                                    rev_timestamp < "%(end)s"
                            """,
}


