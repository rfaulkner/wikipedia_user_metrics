
"""
    Store the query calls for UserMetric classes
"""

__author__ = "Ryan Faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "january 30th, 2013"
__license__ = "GPL (version 2 or later)"

import user_metrics.config.settings as conf

from user_metrics.utils import format_mediawiki_timestamp
from user_metrics.etl.data_loader import DataLoader, Connector, ConnectorError
from MySQLdb import escape_string, ProgrammingError, OperationalError
from copy import deepcopy
from datetime import datetime
from re import sub

from user_metrics.config import logging

DB_TOKEN = '<database>'
TABLE_TOKEN = '<table>'


class UMQueryCallError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Query call failed."):
        Exception.__init__(self, message)


def sub_tokens(query, db='', table=''):
    """
    Substitutes values for portions of queries that specify MySQL databases and
    tables.
    """
    query = sub(DB_TOKEN, db, query)
    query = sub(TABLE_TOKEN, table, query)
    return query


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
        for elem in var:
            escaped_var.append(escape_var(elem))
        return escaped_var
    else:
        return escape_string(''.join(str(var).split()))


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
                ",".join(DataLoader()
                .cast_elems_to_string(escape_var(list(namespace)))) + ')'
    return ns_cond


def query_method_deco(f):
    """ Decorator that handles setup and tear down of user
        query dependent on user cohort & project """
    def wrapper(users, project, args):
        # ensure the handles are iterable
        if not hasattr(users, '__iter__'):
            users = [users]

        # escape project & users
        users = escape_var(users)
        project = escape_var(project)

        # get query and call
        if hasattr(args, 'log') and args.log:
            logging.debug(__name__ + ':: calling "%(method)s" '
                                     'in "%(project)s".' %
                                     {
                                         'method': f.__name__,
                                         'project': project
                                     }
                          )
        # 1. Synthesize query
        # 2. substitute project
        query = f(users, project, args)
        query = sub_tokens(query, db=project)

        try:
            conn = Connector(instance=conf.PROJECT_DB_MAP[project])
        except KeyError:
            logging.error(__name__ + ' :: Project does not exist.')
            return []
        except ConnectorError:
            logging.error(__name__ + ' :: Could not establish a connection.')
            raise UMQueryCallError('Could not establish a connection.')

        try:
            conn._cur_.execute(query)
        except ProgrammingError:
            logging.error(__name__ +
                          ' :: Query failed: {0}'.format(query))
            raise UMQueryCallError()
        results = [row for row in conn._cur_]
        del conn
        return results
    return wrapper


def rev_count_query(uid, is_survival, namespace, project,
                    start_ts, threshold_ts):
    """ Get count of revisions associated with a UID for Threshold metrics """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])

    # The key difference between survival and threshold is that threshold
    # measures a level of activity before a point whereas survival
    # (generally) measures any activity after a point
    if is_survival:
        timestamp_cond = ' and rev_timestamp > %(ts)s'
    else:
        timestamp_cond = ' AND rev_timestamp > ' + \
                         start_ts + ' AND rev_timestamp <= "%(ts)s"'

    # format the namespace condition
    ns_cond = format_namespace(deepcopy(namespace))

    query = query_store[rev_count_query.__name__] + timestamp_cond
    query = sub_tokens(query, db=escape_var(project))

    params = {
        'ts': threshold_ts,
        'ns': ns_cond,
        'uid': int(uid)
    }
    print params
    conn._cur_.execute(query, params)
    try:
        count = int(conn._cur_.fetchone()[0])
    except (IndexError, ValueError):
        raise UMQueryCallError()
    del conn
    return count
rev_count_query.__query_name__ = 'rev_count_query'


@query_method_deco
def live_account_query(users, project, args):
    """ Format query for live_account metric """

    user_cond = DataLoader().format_condition_in('ept_user',
                                                 escape_var(users))
    ns_cond = format_namespace(args.namespace)

    where_clause = 'log_action = "create"'
    if user_cond:
        where_clause += ' AND ' + user_cond
    if ns_cond:
        where_clause += ' AND ' + ns_cond

    from_clause = '%(project)s.edit_page_tracking AS e RIGHT JOIN ' \
                  '%(project)s.logging AS l ON e.ept_user = l.log_user'
    if ns_cond:
        from_clause += " LEFT JOIN %(project)s.page as p " \
                       "ON e.ept_title = p.page_title"
    from_clause = from_clause % {
        "project": escape_var(project)
    }
    query = query_store[live_account_query.__query_name__] % {
        'from_clause': from_clause,
        'where_clause': where_clause,
    }
    return query
live_account_query.__query_name__ = 'live_account_query'


@query_method_deco
def rev_query(users, project, args):
    """ Get revision length, user, and page """
    # Format query conditions
    ts_condition = \
        'rev_timestamp >= "%(date_start)s" AND rev_timestamp < ' \
        '"%(date_end)s"' % {
        'date_start': escape_var(args.date_start),
        'date_end': escape_var(args.date_end)
        }
    user_set = DataLoader().format_comma_separated_list(users,
                                                        include_quotes=False)
    where_clause = 'rev_user in (%(user_set)s) and %(ts_condition)s' % {
        'user_set': user_set,
        'ts_condition': ts_condition
    }
    ns_cond = format_namespace(args.namespace)
    if ns_cond:
        ns_cond += ' and'

    query = query_store[rev_query.__query_name__] % {
        'where_clause': where_clause,
        'namespace': ns_cond}
    return query
rev_query.__query_name__ = 'rev_query'


def rev_len_query(rev_id, project):
    """ Get parent revision length - returns long """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    query = query_store[rev_len_query.__name__] % {
        'parent_rev_id': int(rev_id),
    }
    query = sub_tokens(query, db=escape_var(project))
    conn._cur_.execute(query)
    try:
        rev_len = conn._cur_.fetchone()[0]
    except (IndexError, KeyError, ProgrammingError):
        raise UMQueryCallError()
    del conn
    return rev_len
rev_len_query.__query_name__ = 'rev_len_query'


def rev_user_query(project, start, end):
    """ Produce all users that made a revision within period """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    query = query_store[rev_user_query.__name__] % {
        'start': escape_var(start),
        'end': escape_var(end),
    }
    query = sub_tokens(query, db=escape_var(project))
    conn._cur_.execute(query)
    users = [str(row[0]) for row in conn._cur_]
    del conn
    return users
rev_user_query.__query_name__ = 'rev_user_query'


def page_rev_hist_query(rev_id, page_id, n, project, namespace,
                        look_ahead=False):
    """ Compute revision history pegged to a given rev """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])

    # Format namespace expression and comparator
    ns_cond = format_namespace(namespace)
    comparator = '>' if look_ahead else '<'

    query = query_store[page_rev_hist_query.__name__] % {
        'rev_id':  long(rev_id),
        'page_id': long(page_id),
        'n':       int(n),
        'namespace': ns_cond,
        'comparator': comparator,
    }
    query = sub_tokens(query, db=escape_var(project))
    conn._cur_.execute(query)

    for row in conn._cur_:
        yield row
    del conn
page_rev_hist_query.__query_name__ = 'page_rev_hist_query'


@query_method_deco
def revert_rate_user_revs_query(user, project, args):
    """ Get revision history for a user """
    return query_store[revert_rate_user_revs_query.__query_name__] % {
        'user': user[0],
        'start_ts': escape_var(args.date_start),
        'end_ts': escape_var(args.date_end),
    }
revert_rate_user_revs_query.__query_name__ = 'revert_rate_user_revs_query'


@query_method_deco
def time_to_threshold_revs_query(user_id, project, args):
    """ Obtain revisions to perform threshold computation """
    sql = query_store[time_to_threshold_revs_query.__query_name__] % {
        'user_handle': str(user_id[0])
    }
    return " ".join(sql.strip().splitlines())
time_to_threshold_revs_query.__query_name__ = 'time_to_threshold_revs_query'


def blocks_user_map_query(users, project):
    """ Obtain map to generate uname to uid"""
    # Get usernames for user ids to detect in block events
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    user_str = DataLoader().format_comma_separated_list(
        escape_var(users))

    query = query_store[blocks_user_map_query.__name__] % {
        'users': user_str
    }
    query = sub_tokens(query, db=escape_var(project))
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
    query = query_store[blocks_user_query.__query_name__] % {
        'user_str': user_str,
        'timestamp': escape_var(args.date_start),
    }
    return query
blocks_user_query.__query_name__ = 'blocks_user_query'


@query_method_deco
def edit_count_user_query(users, project, args):
    """  Obtain rev counts by user """
    user_str = DataLoader().format_comma_separated_list(users)
    ts_condition = 'and rev_timestamp >= "%s" and rev_timestamp < "%s"' % \
                   (escape_var(args.date_start),
                   escape_var(args.date_end))
    query = query_store[edit_count_user_query.__query_name__] % {
        'users': user_str,
        'ts_condition': ts_condition,
    }
    return query
edit_count_user_query.__query_name__ = 'edit_count_user_query'


@query_method_deco
def namespace_edits_rev_query(users, project, args):
    """ Obtain revisions by namespace """

    # @TODO check attributes for existence and throw error otherwise

    to_string = DataLoader().cast_elems_to_string
    to_csv_str = DataLoader().format_comma_separated_list

    # Format user condition
    user_str = "rev_user in (" + to_csv_str(to_string(users)) + ")"

    # Format timestamp condition
    ts_cond = "rev_timestamp >= %s and rev_timestamp < %s" % \
        (escape_var(args.start), escape_var(args.end))

    query = query_store[namespace_edits_rev_query.__query_name__] % {
        "user_cond": user_str,
        "ts_cond": ts_cond,
    }
    return query
namespace_edits_rev_query.__query_name__ = 'namespace_edits_rev_query'


@query_method_deco
def user_registration_date_logging(users, project, args):
    """ Returns user registration date from logging table """
    users = DataLoader().cast_elems_to_string(users)
    uid_str = DataLoader().format_comma_separated_list(users,
                                                       include_quotes=False)
    query = query_store[user_registration_date_logging.__query_name__] % {
        "uid": uid_str,
    }
    return " ".join(query.strip().splitlines())
user_registration_date_logging.__query_name__ = \
    'user_registration_date_logging'


@query_method_deco
def user_registration_date_user(users, project, args):
    """ Returns user registration date from user table """
    users = DataLoader().cast_elems_to_string(users)
    uid_str = DataLoader().format_comma_separated_list(users,
                                                       include_quotes=False)
    return query_store[user_registration_date_user.__query_name__] % {
        "uid": uid_str,
    }
user_registration_date_user.__query_name__ = 'user_registration_date_user'


def delete_usertags(ut_tag):
    """
        Delete records from usertags for a give tag ID.  This effectively
        empties a cohort.
    """
    conn = Connector(instance=conf.PROJECT_DB_MAP[
        conf.__cohort_data_instance__])
    del_query = query_store[delete_usertags.__query_name__] % {
        'ut_tag': ut_tag
    }
    del_query = sub_tokens(del_query,
                       db=conf.__cohort_meta_instance__,
                       table=conf.__cohort_db__)
    conn._cur_.execute(del_query)
    conn._db_.commit()
    del conn
delete_usertags.__query_name__ = 'delete_usertags'


def delete_usertags_meta(ut_tag):
    """
        Delete record from usertags_meta for a give tag ID.  This effectively
        deletes a cohort.
    """
    conn = Connector(instance=conf.PROJECT_DB_MAP[
        conf.__cohort_data_instance__])
    del_query = query_store[delete_usertags_meta.__query_name__] % {
        'ut_tag': ut_tag
    }
    del_query = sub_tokens(del_query,
                           db=conf.__cohort_meta_instance__,
                           table=conf.__cohort_meta_db__)
    conn._cur_.execute(del_query)
    conn._db_.commit()
    del conn
delete_usertags_meta.__query_name__ = 'delete_usertags_meta'


def get_api_user(user, by_id=True):
    """
        Retrieve an API user from the ``PROD`` database.

        Parameters
        ~~~~~~~~~~

            user : int|str
                Reference to an API user.

            by_id : Bool(=True)
                Flag to determine whether filtering by id or name.
    """
    conn = Connector(instance=conf.__cohort_data_instance__)

    if by_id:
        query = get_api_user.__query_name__ + '_by_id'
    else:
        query = get_api_user.__query_name__ + '_by_name'
    query = query_store[query] % {
        'user': str(escape_var(user))
    }
    query = sub_tokens(query, db=conf.__cohort_meta_instance__)
    conn._cur_.execute(query)

    api_user_tuple = conn._cur_.fetchone()
    del conn
    return api_user_tuple
get_api_user.__query_name__ = 'get_api_user'


def insert_api_user(user, password):
    """
        Retrieve an API user from the ``PROD`` database.

        Parameters
        ~~~~~~~~~~

            user : int|str
                User name.

            password : string
                Password, this should be a salted hash string.
    """
    conn = Connector(instance=conf.__cohort_data_instance__)
    query = insert_api_user.__query_name__
    query = query_store[query] % {
        'user': str(escape_var(user)),
        'pass': str(escape_var(password))
    }
    query = sub_tokens(query, db=conf.__cohort_meta_instance__)
    conn._cur_.execute(query)
    conn._db_.commit()
    del conn
insert_api_user.__query_name__ = 'insert_api_user'


def add_cohort_data(cohort, users, project,
                    notes="", owner=1, group=3,
                    add_meta=True):
    """
        Adds a new cohort to backend.

        Parameters
        ~~~~~~~~~~

            cohort : string
                Name of cohort (must be unique).

            users : list
                List of user ids to add to cohort.

            project : string
                Project of cohort.
    """
    conn = Connector(instance=conf.__cohort_data_instance__)
    now = format_mediawiki_timestamp(datetime.now())

    # TODO: ALLOW THE COHORT DEF TO BE REFRESHED IF IT ALREADY EXISTS

    if add_meta:
        logging.debug(__name__ + ' :: Adding new cohort "{0}".'.
                      format(cohort))
        if not notes:
            notes = 'Generated by: ' + __name__

        # Create an entry in ``usertags_meta``
        utm_query = query_store[add_cohort_data.__query_name__ + '_meta'] % {
            'utm_name': escape_var(cohort),
            'utm_project': escape_var(project),
            'utm_notes': notes,
            'utm_group': escape_var(str(group)),
            'utm_owner': escape_var(str(owner)),
            'utm_touched': now,
            'utm_enabled': '0'
        }
        utm_query = sub_tokens(utm_query, db=conf.__cohort_meta_instance__,
                               table=conf.__cohort_meta_db__)
        conn._cur_.execute(utm_query)
        try:
            conn._db_.commit()
        except (ProgrammingError, OperationalError):
            conn._db_.rollback()

    # add data to ``user_tags``
    if users:

        # get uid for cohort
        usertag = get_cohort_id(cohort)

        logging.debug(__name__ + ' :: Adding cohort {0} users.'.
                      format(len(users)))
        value_list_ut = [('{0}'.format(project),
                          int(uid),
                          int(usertag))
                         for uid in users]
        value_list_ut = str(value_list_ut)[1:-1]

        ut_query = query_store[add_cohort_data.__query_name__] % {
            'cohort_db': conf.__cohort_db__,
            'value_list': value_list_ut
        }
        ut_query = sub_tokens(ut_query, db=conf.__cohort_meta_instance__)
        conn._cur_.execute(ut_query)
        try:
            conn._db_.commit()
        except (ProgrammingError, OperationalError):
            conn._db_.rollback()

    del conn
add_cohort_data.__query_name__ = 'add_cohort'


def get_cohort_data(cohort_name):
    """
        Returns the cohort tag for a given cohort.

        Parameters
        ~~~~~~~~~~

            cohort_name : string
                Name of cohort.
    """
    conn = Connector(instance=conf.__cohort_data_instance__)
    ut_query = query_store[get_cohort_data.__query_name__] % {
        'utm_name': cohort_name
    }
    ut_query = sub_tokens(ut_query, db=conf.__cohort_meta_instance__,
                           table=conf.__cohort_meta_db__)
    conn._cur_.execute(ut_query)
    data = conn._cur_.fetchone()
    del conn
    return data
get_cohort_data.__query_name__ = 'get_cohort_data'


def get_cohort_id(cohort_name):
    try:
        return get_cohort_data(cohort_name)[0]
    except TypeError:
        return None


def get_cohort_project_by_meta(cohort_name):
    try:
        return get_cohort_data(cohort_name)[1]
    except TypeError:
        return None


def get_cohort_users(tag_id):
    """
        Returns user id list for cohort.

        Parameters
        ~~~~~~~~~~

            cohort_name : string
                Name of cohort.
    """
    conn = Connector(instance=conf.__cohort_data_instance__)
    ut_query = query_store[get_cohort_users.__query_name__] % {
        'tag_id': tag_id
    }
    ut_query = sub_tokens(ut_query, db=conf.__cohort_meta_instance__,
                          table=conf.__cohort_db__)
    try:
        conn._cur_.execute(ut_query)
    except OperationalError:
        raise UMQueryCallError('Failed to retrieve users.')

    for row in conn._cur_:
        yield unicode(row[0])
    del conn
get_cohort_users.__query_name__ = 'get_cohort_users'


def get_mw_user_id(username, project):
    """
    Returns a UID given.

    Parameters
    ~~~~~~~~~~

        username : string
            MediaWiki user name

        project : string
            MediaWiki project.
    """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    query = query_store[get_mw_user_id.__query_name__] % {
        'username': username,
        'project': project
    }
    query = sub_tokens(query, db=escape_var(project))
    conn._cur_.execute(query)
    uid = conn._cur_.fetchone()[0]
    del conn
    return uid
get_mw_user_id.__query_name__ = 'get_mw_user_id'


# QUERY DEFINITIONS
# #################

query_store = {
    rev_count_query.__query_name__:
    """
        SELECT
            count(*) as revs
        FROM <database>.revision as r
            JOIN <database>.page as p
                ON  r.rev_page = p.page_id
        WHERE %(ns)s AND rev_user = %(uid)d
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
        from <database>.revision
            join <database>.page
            on page.page_id = revision.rev_page
        where %(namespace)s %(where_clause)s
    """,
    rev_len_query.__query_name__:
    """
        SELECT rev_len
        FROM <database>.revision
        WHERE rev_id = %(parent_rev_id)s
    """,
    rev_user_query.__query_name__:
    """
        SELECT distinct rev_user
        FROM <database>.revision
        WHERE rev_timestamp >= "%(start)s" AND
            rev_timestamp < "%(end)s"
    """,
    page_rev_hist_query.__query_name__:
    """
        SELECT rev_id, rev_user_text, rev_sha1
        FROM <database>.revision JOIN <database>.page
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
       FROM <database>.revision
       WHERE rev_user = %(user)s AND
       rev_timestamp > "%(start_ts)s" AND
       rev_timestamp <= "%(end_ts)s"
    """,
    time_to_threshold_revs_query.__query_name__:
    """
        SELECT rev_timestamp
        FROM <database>.revision
        WHERE rev_user = "%(user_handle)s"
        ORDER BY 1 ASC
    """,
    blocks_user_map_query.__name__:
    """
        SELECT
            user_id,
            user_name
        FROM <database>.user
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
        FROM <database>.logging
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
        FROM <database>.revision
        WHERE rev_user IN (%(users)s) %(ts_condition)s
        GROUP BY 1
    """,
    namespace_edits_rev_query.__query_name__:
    """
        SELECT
            r.rev_user,
            p.page_namespace,
            count(*) AS revs
        FROM <database>.revision AS r
            JOIN <database>.page AS p
            ON r.rev_page = p.page_id
        WHERE %(user_cond)s AND %(ts_cond)s
        GROUP BY 1,2
    """,
    user_registration_date_logging.__query_name__:
    """
        SELECT
            log_user,
            log_timestamp
        FROM <database>.logging
        WHERE (log_action = 'create' OR
            log_action = 'autocreate') AND
            log_type='newusers' AND
            log_user in (%(uid)s)
    """,
    user_registration_date_user.__query_name__:
    """
        SELECT
            user_id,
            user_registration
        FROM <database>.user
        WHERE user_id in (%(uid)s)
    """,
    delete_usertags.__query_name__:
    """
        DELETE FROM <database>.<table>
        WHERE ut_tag = %(ut_tag)s
    """,
    delete_usertags_meta.__query_name__:
    """
        DELETE FROM
            <database>.<database>
        WHERE ut_tag = %(ut_tag)s
    """,
    get_api_user.__query_name__ + '_by_id':
    """
        SELECT user_name, user_id, user_pass
        FROM <database>.api_user
        WHERE user_id = %(user)s
    """,
    get_api_user.__query_name__ + '_by_name':
    """
        SELECT user_name, user_id, user_pass
        FROM <database>.api_user
        WHERE user_name = '%(user)s'
    """,
    insert_api_user.__query_name__:
    """
        INSERT INTO <database>.api_user
            (user_name, user_pass)
        VALUES ("%(user)s", "%(pass)s")
    """,
    add_cohort_data.__query_name__:
    """
        INSERT INTO <database>.<table>
            VALUES %(value_list)s
    """,
    add_cohort_data.__query_name__ + '_meta':
    """
        INSERT INTO <database>.<table>
            (utm_name, utm_project, utm_notes, utm_group, utm_owner,
            utm_touched, utm_enabled)
        VALUES ("%(utm_name)s", "%(utm_project)s",
            "%(utm_notes)s", "%(utm_group)s", %(utm_owner)s,
            "%(utm_touched)s", %(utm_enabled)s)
    """,
    get_cohort_data.__query_name__:
    """
        SELECT utm_id, utm_project
        FROM <database>.<table>
        WHERE utm_name = "%(utm_name)s"
    """,
    get_mw_user_id.__query_name__:
    """
        SELECT user_id
        FROM <database>.user
        WHERE user_name = "%(username)s"
    """,
    get_cohort_users.__query_name__:
    """
        SELECT ut_user
        FROM <database>.<table>
        WHERE ut_tag = %(tag_id)s
    """,
}
