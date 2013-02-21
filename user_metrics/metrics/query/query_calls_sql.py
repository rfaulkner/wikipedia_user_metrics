
"""
    Store the query calls for UserMetric classes
"""

__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "january 30th, 2013"
__license__ = "GPL (version 2 or later)"

from user_metrics.etl.data_loader import DataLoader, Connector, DB_MAP
from MySQLdb import escape_string, ProgrammingError
from copy import deepcopy

from user_metrics.config import logging

class UMQueryCallError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Query call failed."):
        Exception.__init__(self, message)

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

    # Copy so as not to affect mutable ref
    namespace = deepcopy(namespace)

    if hasattr(namespace, '__iter__'):
        if len(namespace) == 1:
            ns_cond = 'page_namespace = ' + escape_var(str(namespace.pop()))
        else:
            ns_cond = 'page_namespace in (' + \
                      ",".join(DataLoader().
                                cast_elems_to_string(
                          escape_var(list(namespace)))) + ')'
    return ns_cond

def query_method_deco(f):
    """ Decorator that handles setup and tear down of user
        query dependent on user cohort & project """
    def wrapper(users, project, args):
        # ensure the handles are iterable
        if not hasattr(users, '__iter__'): users = [users]

        # get query and call
        if hasattr(args, 'log') and args.log:
            logging.debug(__name__ + ':: calling "%(method)s" in "%(project)s".' %
                                     {
                                        'method': f.__name__,
                                        'project': project
            })
        # Call query escaping user and project variables for SQL injection
        query = f(escape_var(users), escape_var(project), args)

        try:
            conn = Connector(instance=DB_MAP[project])
        except KeyError:
            logging.error(__name__ + '::Project does not exist.')
            return []

        try:
            conn._cur_.execute(query)
        except ProgrammingError:
            logging.error(__name__ +
                          'Could not get edit counts - Query failed.')
            raise UMQueryCallError()
        results = [row for row in conn._cur_]
        del conn
        return results
    return wrapper

def rev_count_query(uid, is_survival, namespace, project,
                    restrict, start_time, end_time, threshold_ts):
    """ Get count of revisions associated with a UID for Threshold metrics """
    conn = Connector(instance=DB_MAP[project])

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
                          'rev_timestamp <= "{1}"'.format(
            escape_var(start_time), escape_var(end_time))

    query = query_store[rev_count_query.__name__] + timestamp_cond
    query = query % {'project' : project,
                'ts' : escape_var(threshold_ts),
                'ns' : ns_cond,
                'uid' : long(uid)
    }
    query =  " ".join(query.strip().splitlines())
    conn._cur_.execute(query)
    try:
        count = int(conn._cur_.fetchone()[0])
    except IndexError:
        raise UMQueryCallError()
    except ValueError:
        raise UMQueryCallError()
    return count
rev_count_query.__query_name__ = 'rev_count_query'

@query_method_deco
def live_account_query(users, project, args):
    """ Format query for live_account metric """

    user_cond = DataLoader().format_condition_in('ept_user', users)
    ns_cond = format_namespace(args.namespace)

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
    return " ".join(sql.strip().splitlines())
live_account_query.__query_name__ = 'live_account_query'

@query_method_deco
def rev_query(users, project, args):
    """ Get revision length, user, and page """
    # Format query conditions
    ts_condition  = 'rev_timestamp >= "%(date_start)s" AND '\
                    'rev_timestamp < "%(date_end)s"' %\
                    {
                        'date_start': escape_var(args.date_start),
                        'date_end': escape_var(args.date_end)
                    }
    user_set = DataLoader().format_comma_separated_list(users,
        include_quotes=False)
    where_clause = 'rev_user in (%(user_set)s) and %(ts_condition)s' % {
        'user_set' : user_set, 'ts_condition' : ts_condition}
    ns_cond = format_namespace(args.namespace)
    if ns_cond: ns_cond += ' and'

    query = query_store[rev_query.__query_name__] % {
        'where_clause' : where_clause,
        'project' : project,
        'namespace' : ns_cond}
    query = " ".join(query.strip().splitlines())
    return query
rev_query.__query_name__ = 'rev_query'

def rev_len_query(rev_id, project):
    """ Get parent revision length - returns long """
    conn = Connector(instance=DB_MAP[project])
    query = query_store[rev_len_query.__name__] % {
        'project' : escape_var(project),
        'parent_rev_id' : int(rev_id),
    }
    query = " ".join(query.strip().splitlines())
    conn._cur_.execute(query)
    try:
        rev_len = conn._cur_.fetchone()[0]
    except IndexError:
        raise UMQueryCallError()
    except KeyError:
        raise UMQueryCallError()
    except ProgrammingError:
        raise UMQueryCallError()
    return rev_len
rev_len_query.__query_name__ = 'rev_len_query'

def rev_user_query(project, start, end):
    """ Produce all users that made a revision within period """
    conn = Connector(instance=DB_MAP[project])
    query = query_store[rev_user_query.__name__] % \
        {
            'start': escape_var(start),
            'end': escape_var(end),
        }
    query = " ".join(query.strip().splitlines())
    conn._cur_.execute(query)
    users = [str(row[0]) for row in conn._cur_]
    return users
rev_user_query.__query_name__ = 'rev_user_query'

def page_rev_hist_query(rev_id, page_id, n, project, namespace,
                        look_ahead=False):
    """ Compute revision history pegged to a given rev """
    conn = Connector(instance=DB_MAP[project])

    # Format namespace expression and comparator
    ns_cond = format_namespace(namespace)
    comparator =  '>' if look_ahead else '<'

    conn._cur_.execute(query_store[page_rev_hist_query.__name__] %
                       {
                        'rev_id':  long(rev_id),
                        'page_id': long(page_id),
                        'n':       int(n),
                        'project': escape_var(project),
                        'namespace': ns_cond,
                        'comparator': comparator,
    })
    for row in conn._cur_:
        yield row
    del conn
page_rev_hist_query.__query_name__ = 'page_rev_hist_query'

@query_method_deco
def revert_rate_user_revs_query(user, project, args):
    """ Get revision history for a user """
    return query_store[revert_rate_user_revs_query.__query_name__] % \
    {
        'user' : user[0],
        'project' : project,
        'start_ts' : escape_var(args.date_start),
        'end_ts' : escape_var(args.date_end),
    }
revert_rate_user_revs_query.__query_name__ = 'revert_rate_user_revs_query'

@query_method_deco
def time_to_threshold_revs_query(user_id, project, args):
    """ Obtain revisions to perform threshold computation """
    sql = query_store[time_to_threshold_revs_query.__query_name__] % {
        'user_handle' : str(user_id[0]),
        'project' : project}
    return " ".join(sql.strip().splitlines())
time_to_threshold_revs_query.__query_name__ = 'time_to_threshold_revs_query'

def blocks_user_map_query(users):
    """ Obtain map to generate uname to uid"""
    # Get usernames for user ids to detect in block events
    conn = Connector(instance='slave')
    user_str = DataLoader().format_comma_separated_list(
        escape_var(users))

    query = query_store[blocks_user_map_query.__name__] % \
        { 'users': user_str }
    query = " ".join(query.strip().splitlines())
    conn._cur_.execute(query)

    # keys username on userid
    user_map = dict()
    for r in conn._cur_:
        user_map[r[1]] = r[0]
    del conn
    return user_map

@query_method_deco
def blocks_user_query(users, project, args):
    """ Obtain block/ban events for users """
    user_str = DataLoader().format_comma_separated_list(users)
    query = query_store[blocks_user_query.__query_name__] % \
                       {
                           'user_str' : user_str,
                           'timestamp': escape_var(args.date_start),
                           'project' : project
                       }
    query = " ".join(query.strip().splitlines())
    return query
blocks_user_query.__query_name__ = 'blocks_user_query'

@query_method_deco
def edit_count_user_query(users, project, args):
    """  Obtain rev counts by user """
    user_str = DataLoader().format_comma_separated_list(users)
    ts_condition  = 'and rev_timestamp >= "%s" and rev_timestamp < "%s"' % \
                        (escape_var(args.date_start),
                         escape_var(args.date_end))
    query  = query_store[edit_count_user_query.__query_name__] % \
                    {
                        'users' : user_str,
                        'ts_condition' : ts_condition,
                        'project' : project
                    }
    query = " ".join(query.strip().splitlines())
    return query
edit_count_user_query.__query_name__ = 'edit_count_user_query'

@query_method_deco
def namespace_edits_rev_query(users, project, args):
    """ Obtain revisions by namespace """

    # @TODO check attributes for existence and throw error otherwise
    start = args.date_start
    end = args.date_end

    to_string = DataLoader().cast_elems_to_string
    to_csv_str = DataLoader().format_comma_separated_list

    # Format user condition
    user_str = "rev_user in (" + to_csv_str(to_string(users)) + ")"

    # Format timestamp condition
    ts_cond = "rev_timestamp >= %s and rev_timestamp < %s" % \
    (escape_var(start), escape_var(end))

    query = query_store[namespace_edits_rev_query.__query_name__] % \
        {
            "user_cond" : user_str,
            "ts_cond" : ts_cond,
            "project" : project,
        }
    query = " ".join(query.strip().splitlines())
    return query
namespace_edits_rev_query.__query_name__ = 'namespace_edits_rev_query'

@query_method_deco
def user_registration_date(users, project, args):
    """ Returns user registration date from logging table """
    users = DataLoader().cast_elems_to_string(users)
    uid_str = DataLoader().format_comma_separated_list(users,
                                                       include_quotes=False)
    query = query_store[user_registration_date.__query_name__] %\
            {
                "uid" : uid_str,
                "project" : project,
            }
    return " ".join(query.strip().splitlines())
user_registration_date.__query_name__ = 'user_registration_date'

query_store = {
    rev_count_query.__query_name__:
                            """
                                SELECT
                                    count(*) as revs
                                FROM %(project)s.revision as r
                                    JOIN %(project)s.page as p
                                        ON  r.rev_page = p.page_id
                                WHERE %(ns)s AND rev_user = %(uid)s
                            """,
    live_account_query.__query_name__:
                            """
                                SELECT
                                    e.ept_user,
                                    MIN(l.log_timestamp) as registration,
                                    MIN(e.ept_timestamp) as first_click
                                FROM %(from_clause)s
                                WHERE %(where_clause)s
                                GROUP BY 1
                            """,
    rev_query.__query_name__:
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
    rev_len_query.__query_name__:
                            """
                                SELECT rev_len
                                FROM %(project)s.revision
                                WHERE rev_id = %(parent_rev_id)s
                            """,
    rev_user_query.__query_name__:
                            """
                                SELECT distinct rev_user
                                FROM enwiki.revision
                                WHERE rev_timestamp >= "%(start)s" AND
                                    rev_timestamp < "%(end)s"
                            """,
    page_rev_hist_query.__query_name__:
                        """
                            SELECT rev_id, rev_user_text, rev_sha1
                            FROM %(project)s.revision JOIN %(project)s.page
                                ON rev_page = page_id
                            WHERE rev_page = %(page_id)s
                                AND rev_id %(comparator)s %(rev_id)s
                                AND %(namespace)s
                            ORDER BY rev_id ASC
                            LIMIT %(n)s
                        """,
    revert_rate_user_revs_query.__query_name__:
                        """
                           SELECT
                               rev_user,
                               rev_page,
                               rev_sha1,
                               rev_user_text
                           FROM %(project)s.revision
                           WHERE rev_user = %(user)s AND
                           rev_timestamp > "%(start_ts)s" AND
                           rev_timestamp <= "%(end_ts)s"
                        """,
    time_to_threshold_revs_query.__query_name__:
                        """
                            SELECT rev_timestamp
                            FROM %(project)s.revision
                            WHERE rev_user = "%(user_handle)s"
                            ORDER BY 1 ASC
                        """,
    blocks_user_map_query.__name__:
                        """
                            SELECT
                                user_id,
                                user_name
                            FROM enwiki.user
                            WHERE user_id in (%(users)s)
                        """,
    blocks_user_query.__query_name__:
                        """
                            SELECT
                                log_title as user,
                                IF(log_params LIKE "%%indefinite%%", "ban",
                                    "block") as type,
                                count(*) as count,
                                min(log_timestamp) as first,
                                max(log_timestamp) as last
                            FROM %(project)s.logging
                            WHERE log_type = "block"
                            AND log_action = "block"
                            AND log_title in (%(user_str)s)
                            AND log_timestamp >= "%(timestamp)s"
                            GROUP BY 1, 2
                        """,
    edit_count_user_query.__query_name__:
                        """
                            SELECT
                                rev_user,
                                count(*)
                            FROM %(project)s.revision
                            WHERE rev_user IN (%(users)s) %(ts_condition)s
                            GROUP BY 1
                        """,
    namespace_edits_rev_query.__query_name__:
                        """
                            SELECT
                                r.rev_user,
                                p.page_namespace,
                                count(*) AS revs
                            FROM %(project)s.revision AS r JOIN %(project)s.page AS p
                                ON r.rev_page = p.page_id
                            WHERE %(user_cond)s AND %(ts_cond)s
                            GROUP BY 1,2
                        """,
    user_registration_date.__query_name__:
                        """
                            SELECT
                                log_user,
                                log_timestamp
                            FROM %(project)s.logging
                            WHERE (log_action = 'create' OR
                                log_action = 'autocreate') AND
                                log_type='newusers' AND
                                log_user in (%(uid)s)
                        """,
}


