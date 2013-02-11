
"""
    Store the query calls for UserMetric classes
"""

__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "january 30th, 2013"
__license__ = "GPL (version 2 or later)"

from src.etl.data_loader import DataLoader, Connector, DB_MAP
from MySQLdb import escape_string, ProgrammingError
from copy import deepcopy

from config import logging

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

def query_method_deco(f):
    """ Decorator that handles setup and tear down of user
        query dependent on user cohort & project """
    def wrapper(users, project, args):
        # Escape user_handle for SQL injection
        users = escape_var(users)

        # ensure the handles are iterable
        if not hasattr(users, '__iter__'): users = [users]

        # get query and call
        logging.debug(__name__ + ':: calling "%(method)s" in "%(project)s".' %
                                 {
                                    'method': f.__name__,
                                    'project': project
        })
        query = f(users, project, args)

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
        results = [row for row in conn._cur_]
        del conn
        return results
    return wrapper


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

def revert_rate_past_revs_query(rev_id, page_id, n, project):
    """ Compute revision history pegged to a given rev """
    conn = Connector(instance=DB_MAP[project])
    conn._cur_.execute(query_store[revert_rate_past_revs_query.__name__] %
                       {
                        'rev_id':  rev_id,
                        'page_id': page_id,
                        'n':       n,
                        'project': project
    })
    for row in conn._cur_:
        yield row
    del conn

def revert_rate_future_revs_query(rev_id, page_id, n, project):
    """ Compute revision future pegged to a given rev """
    conn = Connector(instance=DB_MAP[project])
    conn._cur_.execute(
                        query_store[revert_rate_future_revs_query.__name__] %
                       {
                        'rev_id':  rev_id,
                        'page_id': page_id,
                        'n':       n,
                        'project': project
    })
    for row in conn._cur_:
        yield row
    del conn

def revert_rate_user_revs_query(project, user, start, end):
    """ Get revision history for a user """
    conn = Connector(instance=DB_MAP[project])
    conn._cur_.execute(
                        query_store[revert_rate_user_revs_query.__name__] %
                        {
                            'project' : project,
                            'user' : user,
                            'start_ts' : start,
                            'end_ts' : end
    })
    revisions = [rev for rev in conn._cur_]
    del conn
    return  revisions

@query_method_deco
def time_to_threshold_revs_query(user_id, project, args):
    """ Obtain revisions to perform threshold computation """
    sql = query_store[time_to_threshold_revs_query.__name__] % {
        'user_handle' : str(user_id[0]),
        'project' : project}
    return " ".join(sql.strip().splitlines())

def blocks_user_map_query(users):
    """ Obtain map to generate uname to uid"""
    # Get usernames for user ids to detect in block events
    conn = Connector(instance='slave')
    user_str = DataLoader().format_comma_separated_list(users)

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
                           'timestamp': args.date_start,
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
                        (args.date_start, args.date_end)
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
    ts_cond = "rev_timestamp >= %s and rev_timestamp < %s" % (start, end)

    query = query_store[namespace_edits_rev_query.__query_name__] % \
        {
            "user_cond" : user_str,
            "ts_cond" : ts_cond,
            "project" : project,
        }
    query = " ".join(query.strip().splitlines())
    return query
namespace_edits_rev_query.__query_name__ = 'namespace_edits_rev_query'

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
    revert_rate_past_revs_query.__name__:
                        """
                            SELECT rev_id, rev_user_text, rev_sha1
                            FROM %(project)s.revision
                            WHERE rev_page = %(page_id)s
                                AND rev_id < %(rev_id)s
                            ORDER BY rev_id DESC
                            LIMIT %(n)s
                        """,
    revert_rate_future_revs_query.__name__:
                        """
                            SELECT rev_id, rev_user_text, rev_sha1
                            FROM %(project)s.revision
                            WHERE rev_page = %(page_id)s
                                AND rev_id > %(rev_id)s
                            ORDER BY rev_id ASC
                            LIMIT %(n)s
                        """,
    revert_rate_user_revs_query.__name__:
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
    time_to_threshold_revs_query.__name__:
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
}


