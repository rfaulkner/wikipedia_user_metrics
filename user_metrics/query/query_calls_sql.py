
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
FROM_TOKEN = '<from>'
WHERE_TOKEN = '<where>'
COMP1_TOKEN = '<comparator_1>'
USERS_TOKEN = '<users>'


class UMQueryCallError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Query call failed."):
        Exception.__init__(self, message)


def sub_tokens(query, db='', table='', from_repl='', where='',
               comp_1='', users=''):
    """
    Substitutes values for portions of queries that specify MySQL databases and
    tables.
    """
    tokens = {
        DB_TOKEN: db,
        TABLE_TOKEN: table,
        FROM_TOKEN: from_repl,
        WHERE_TOKEN: where,
        COMP1_TOKEN: comp_1,
        USERS_TOKEN: users,
    }
    for token in tokens:
        token_value = tokens[token]
        if token_value:
            query = sub(token, token_value, query)
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

        ** THIS METHOD ONLY EMITS SQL SAFE STRINGS **
    """

    # If the input is a list recursively call on elements
    if hasattr(var, '__iter__'):
        escaped_var = list()
        for elem in var:
            escaped_var.append(escape_var(elem))
        return escaped_var
    else:
        return escape_string(''.join(str(var).split()))


def format_namespace(namespace, col='page_namespace'):
    """ Format the namespace condition in queries and returns the string.

        Expects a list of numeric namespace keys.  Otherwise returns
        an empty condition string.

        ** THIS METHOD ONLY EMITS SQL SAFE STRINGS **
    """
    ns_cond = ''

    # Copy so as not to affect mutable ref
    namespace = deepcopy(namespace)

    if hasattr(namespace, '__iter__'):
        if len(namespace) == 1:
            ns_cond = '{0} = '.format(col) \
                + escape_var(str(namespace.pop()))
        else:
            ns_cond = '{0} in ('.format(col) + \
                ",".join(DataLoader()
                .cast_elems_to_string(escape_var(list(namespace)))) + ')'
    else:
        try:
            ns_cond = '{0} = '.format(col) + escape_var(int(namespace))
        except ValueError:
            # No namespace condition
            logging.error(__name__ + ' :: Could not apply namespace '
                                     'condition on {0}'.format(str(namespace)))
            pass

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

        # compose a csv of user ids
        user_str = DataLoader().format_comma_separated_list(users)

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
        query, params = f(users, project, args)
        query = sub_tokens(query, db=project, users=user_str)
        try:
            conn = Connector(instance=conf.PROJECT_DB_MAP[project])
        except KeyError:
            logging.error(__name__ + ' :: Project does not exist.')
            return []
        except ConnectorError:
            logging.error(__name__ + ' :: Could not establish a connection.')
            raise UMQueryCallError(__name__ + ' :: Could not '
                                              'establish a connection.')

        try:
            if params:
                conn._cur_.execute(query, params)
            else:
                conn._cur_.execute(query)
        except (OperationalError, ProgrammingError) as e:
            logging.error(__name__ +
                          ' :: Query failed: {0}, params = {1}'.
                          format(query, str(params)))
            raise UMQueryCallError(__name__ + ' :: ' + str(e))
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
        timestamp_cond = ' AND rev_timestamp > "' + \
                         escape_var(start_ts) + '" AND ' + \
                         'rev_timestamp <= %(ts)s'

    # format the namespace condition
    ns_cond = format_namespace(deepcopy(namespace))

    query = query_store[rev_count_query.__name__] + timestamp_cond
    query = sub_tokens(query, db=escape_var(project), where=ns_cond)
    conn._cur_.execute(query, {'uid': int(uid), 'ts': str(threshold_ts)})
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

    try:
        ns_cond = format_namespace(args.namespace, col='e.ept_namespace')
        if ns_cond:
            ns_cond = ' AND ' + ns_cond
    except AttributeError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

    query = query_store[live_account_query.__query_name__]
    query = sub_tokens(query, where=ns_cond)
    return query, None
live_account_query.__query_name__ = 'live_account_query'


@query_method_deco
def rev_query(users, project, args):
    """ Get revision length, user, and page """
    # Format query conditions
    try:
        ts_condition = \
            'rev_timestamp >= "%(date_start)s" AND rev_timestamp < ' \
            '"%(date_end)s"' % {
            'date_start': escape_var(args.date_start),
            'date_end': escape_var(args.date_end)
            }
    except AttributeError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

    user_set = DataLoader().format_comma_separated_list(users,
                                                        include_quotes=False)
    where_clause = 'rev_user in (%(user_set)s) and %(ts_condition)s' % {
        'user_set': user_set,
        'ts_condition': ts_condition
    }

    try:
        ns_cond = format_namespace(args.namespace)
    except AttributeError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

    if ns_cond:
        ns_cond += ' and '
    where_clause = ns_cond + where_clause
    query = query_store[rev_query.__query_name__]
    query = sub_tokens(query, db=escape_var(project), where=where_clause)
    return query, None
rev_query.__query_name__ = 'rev_query'


def rev_len_query(rev_id, project):
    """ Get parent revision length - returns long """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    query = query_store[rev_len_query.__name__]
    query = sub_tokens(query, db=escape_var(project))
    conn._cur_.execute(query, {'parent_rev_id': int(rev_id)})
    try:
        rev_len = conn._cur_.fetchone()[0]
    except (IndexError, KeyError, ProgrammingError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
    del conn
    return rev_len
rev_len_query.__query_name__ = 'rev_len_query'


def rev_user_query(project, start, end):
    """ Produce all users that made a revision within period """
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    query = query_store[rev_user_query.__name__]
    query = sub_tokens(query, db=escape_var(project))
    params = {
        'start': str(start),
        'end': str(end)
    }
    conn._cur_.execute(query, params)
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
    query = query_store[page_rev_hist_query.__name__]
    query = sub_tokens(query, db=escape_var(project),
                       comp_1=comparator, where=ns_cond)
    try:
        params = {
            'rev_id':  long(rev_id),
            'page_id': long(page_id),
            'n':       int(n),
        }
    except ValueError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

    conn._cur_.execute(query, params)
    for row in conn._cur_:
        yield row
    del conn
page_rev_hist_query.__query_name__ = 'page_rev_hist_query'


@query_method_deco
def revert_rate_user_revs_query(user, project, args):
    """ Get revision history for a user """
    ns_cond = format_namespace(args.namespace)
    query = query_store[revert_rate_user_revs_query.__query_name__]
    query = sub_tokens(query, where=ns_cond)
    try:
        params = {
            'user': int(user[0]),
            'start_ts': str(args.date_start),
            'end_ts': str(args.date_end),
        }
    except ValueError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
    return query, params
revert_rate_user_revs_query.__query_name__ = 'revert_rate_user_revs_query'


@query_method_deco
def time_to_threshold_revs_query(user_id, project, args):
    """ Obtain revisions to perform threshold computation """
    query = query_store[time_to_threshold_revs_query.__query_name__]
    params = {'user_handle': str(user_id[0])}
    return query, params
time_to_threshold_revs_query.__query_name__ = 'time_to_threshold_revs_query'


def blocks_user_map_query(users, project):
    """ Obtain map to generate uname to uid"""
    # Get usernames for user ids to detect in block events
    conn = Connector(instance=conf.PROJECT_DB_MAP[project])
    user_str = DataLoader().format_comma_separated_list(
        escape_var(users))

    query = query_store[blocks_user_map_query.__name__]
    query = sub_tokens(query, db=escape_var(project), users=user_str)
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
    query = query_store[blocks_user_query.__query_name__]
    try:
        params = {'timestamp': str(args.date_start)}
    except AttributeError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
    return query, params
blocks_user_query.__query_name__ = 'blocks_user_query'


@query_method_deco
def edit_count_user_query(users, project, args):
    """  Obtain rev counts by user """
    query = query_store[edit_count_user_query.__query_name__]
    try:
        params = {'start': str(args.date_start), 'end': str(args.date_end)}
    except AttributeError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
    return query, params
edit_count_user_query.__query_name__ = 'edit_count_user_query'


@query_method_deco
def namespace_edits_rev_query(users, project, args):
    """ Obtain revisions by namespace """
    query = query_store[namespace_edits_rev_query.__query_name__]
    try:
        params = {'start': str(args.start), 'end': str(args.end)}
    except AttributeError as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
    return query, params
namespace_edits_rev_query.__query_name__ = 'namespace_edits_rev_query'


@query_method_deco
def user_registration_date_logging(users, project, args):
    """ Returns user registration date from logging table """
    return query_store[user_registration_date_logging.__query_name__], None
user_registration_date_logging.__query_name__ = \
    'user_registration_date_logging'


@query_method_deco
def user_registration_date_user(users, project, args):
    """ Returns user registration date from user table """
    return query_store[user_registration_date_user.__query_name__], None
user_registration_date_user.__query_name__ = 'user_registration_date_user'


def delete_usertags(ut_tag):
    """
        Delete records from usertags for a give tag ID.  This effectively
        empties a cohort.
    """
    conn = Connector(instance=conf.PROJECT_DB_MAP[
        conf.__cohort_data_instance__])
    del_query = query_store[delete_usertags.__query_name__]
    del_query = sub_tokens(del_query,
                           db=conf.__cohort_meta_instance__,
                           table=conf.__cohort_db__)
    try:
        conn._cur_.execute(del_query, {'ut_tag': int(ut_tag)})
    except (ValueError, ProgrammingError, OperationalError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
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
    del_query = query_store[delete_usertags_meta.__query_name__]
    del_query = sub_tokens(del_query,
                           db=conf.__cohort_meta_instance__,
                           table=conf.__cohort_meta_db__)
    try:
        conn._cur_.execute(del_query, {'ut_tag': int(ut_tag)})
    except (ValueError, ProgrammingError, OperationalError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
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
        try:
            params = {'user': int(user)}
        except ValueError as e:
            raise UMQueryCallError(__name__ + ' :: ' + str(e))
    else:
        query = get_api_user.__query_name__ + '_by_name'
        params = {'user': str(user)}
    query = query_store[query]
    query = sub_tokens(query, db=conf.__cohort_meta_instance__)

    try:
        conn._cur_.execute(query, params)
    except (ValueError, ProgrammingError, OperationalError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

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
    query = query_store[query]
    params = {
        'user': str(user),
        'pass': str(password)
    }
    query = sub_tokens(query, db=conf.__cohort_meta_instance__)

    try:
        conn._cur_.execute(query, params)
    except (ProgrammingError, OperationalError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

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
        utm_query = query_store[add_cohort_data.__query_name__ + '_meta']

        try:
            params = {
                'utm_name': str(cohort),
                'utm_project': str(project),
                'utm_notes': str(notes),
                'utm_group': int(group),
                'utm_owner': int(owner),
                'utm_touched': now,
                'utm_enabled': 0
            }
        except ValueError as e:
            raise UMQueryCallError(__name__ + ' :: ' + str(e))

        utm_query = sub_tokens(utm_query, db=conf.__cohort_meta_instance__,
                               table=conf.__cohort_meta_db__)
        try:
            conn._cur_.execute(utm_query, params)
            conn._db_.commit()
        except (ProgrammingError, OperationalError) as e:
            conn._db_.rollback()
            raise UMQueryCallError(__name__ + ' :: ' + str(e))

    # add data to ``user_tags``
    if users:
        # get uid for cohort
        usertag = get_cohort_id(cohort)

        logging.debug(__name__ + ' :: Adding cohort {0} users.'.
                      format(len(users)))

        try:
            value_list_ut = [('{0}'.format(project),
                              int(uid),
                              int(usertag))
                             for uid in users]
        except ValueError as e:
            raise UMQueryCallError(__name__ + ' :: ' + str(e))

        ut_query = query_store[add_cohort_data.__query_name__] + '(' + \
                   ' %s,' * len(value_list_ut)[:-1] + ')'
        ut_query = sub_tokens(ut_query, db=conf.__cohort_meta_instance__,
                              table=conf.__cohort_db__)
        try:
            conn._cur_.execute(ut_query, value_list_ut)
            conn._db_.commit()
        except (ProgrammingError, OperationalError) as e:
            conn._db_.rollback()
            raise UMQueryCallError(__name__ + ' :: ' + str(e))
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
    ut_query = query_store[get_cohort_data.__query_name__]
    ut_query = sub_tokens(ut_query, db=conf.__cohort_meta_instance__,
                           table=conf.__cohort_meta_db__)

    try:
        conn._cur_.execute(ut_query, {'utm_name': str(cohort_name)})
    except (ValueError, ProgrammingError, OperationalError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))
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
    ut_query = query_store[get_cohort_users.__query_name__]
    ut_query = sub_tokens(ut_query, db=conf.__cohort_meta_instance__,
                          table=conf.__cohort_db__)
    try:
        conn._cur_.execute(ut_query, {'tag_id': int(tag_id)})
    except (ValueError, ProgrammingError, OperationalError):
        raise UMQueryCallError(__name__ + ' :: Failed to retrieve users.')

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
    query = query_store[get_mw_user_id.__query_name__]
    query = sub_tokens(query, db=escape_var(project))

    try:
        conn._cur_.execute(query, {'username': str(username)})
        uid = conn._cur_.fetchone()[0]
    except (IndexError, ValueError, ProgrammingError,
            OperationalError, TypeError) as e:
        raise UMQueryCallError(__name__ + ' :: ' + str(e))

    del conn
    return uid
get_mw_user_id.__query_name__ = 'get_mw_user_id'


@query_method_deco
def get_latest_user_activity(users, project, args):
    return query_store[get_latest_user_activity.__query_name__], None
get_latest_user_activity.__query_name__ = 'get_latest_user_activity'


# QUERY DEFINITIONS
# #################

query_store = {
    rev_count_query.__query_name__:
    """
        SELECT
            count(*) as revs
        FROM <database>.revision as r
            JOIN <database>.page as p
                ON r.rev_page = p.page_id
        WHERE <where> AND rev_user = %(uid)s
    """,
    live_account_query.__query_name__:
    """
        SELECT
            l.log_user,
            MIN(l.log_timestamp) as registration,
            MIN(e.ept_timestamp) as first_click
        FROM <database>.logging AS l
            LEFT JOIN <database>.edit_page_tracking AS e
            ON e.ept_user = l.log_user
        WHERE (log_action = "create" OR log_action = "autocreate")
            AND log_user in (<users>) <where>
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
        where <where>
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
        WHERE rev_timestamp >= %(start)s AND
            rev_timestamp < %(end)s
    """,
    page_rev_hist_query.__query_name__:
    """
        SELECT rev_id, rev_user_text, rev_sha1
        FROM <database>.revision JOIN <database>.page
            ON rev_page = page_id
        WHERE rev_page = %(page_id)s
            AND rev_id <comparator_1> %(rev_id)s
            AND <where>
        ORDER BY rev_id ASC
        LIMIT %(n)s
    """,
    revert_rate_user_revs_query.__query_name__:
    """
           SELECT
               r.rev_user,
               r.rev_page,
               r.rev_sha1,
               r.rev_user_text
           FROM <database>.revision as r
                JOIN <database>.page as p
                ON r.rev_page = p.page_id
           WHERE r.rev_user = %(user)s AND
           r.rev_timestamp > %(start_ts)s AND
           r.rev_timestamp <= %(end_ts)s AND
           <where>
    """,
    time_to_threshold_revs_query.__query_name__:
    """
        SELECT rev_timestamp
        FROM <database>.revision
        WHERE rev_user = %(user_handle)s
        ORDER BY 1 ASC
    """,
    blocks_user_map_query.__name__:
    """
        SELECT
            user_id,
            user_name
        FROM <database>.user
        WHERE user_id in (<users>)
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
        AND log_title in (<users>)
        AND log_timestamp >= %(timestamp)s
        GROUP BY 1, 2
    """,
    edit_count_user_query.__query_name__:
    """
        SELECT
            rev_user,
            count(*)
        FROM <database>.revision
        WHERE rev_user IN (<users>)
            AND rev_timestamp >= %(start)s
            AND rev_timestamp < %(end)s
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
        WHERE rev_user in (<users>)
            AND rev_timestamp >= %(start)s
            AND rev_timestamp < %(end)s
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
            log_user in (<users>)
    """,
    user_registration_date_user.__query_name__:
    """
        SELECT
            user_id,
            user_registration
        FROM <database>.user
        WHERE user_id in (<users>)
    """,
    delete_usertags.__query_name__:
    """
        DELETE FROM <database>.<table>
        WHERE ut_tag = %(ut_tag)s
    """,
    delete_usertags_meta.__query_name__:
    """
        DELETE FROM
            <database>.<table>
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
        WHERE user_name = %(user)s
    """,
    insert_api_user.__query_name__:
    """
        INSERT INTO <database>.api_user
            (user_name, user_pass)
        VALUES (%(user)s, %(pass)s)
    """,
    add_cohort_data.__query_name__:
    """
        INSERT INTO <database>.<table>
            VALUES
    """,
    add_cohort_data.__query_name__ + '_meta':
    """
        INSERT INTO <database>.<table>
            (utm_name, utm_project, utm_notes, utm_group, utm_owner,
            utm_touched, utm_enabled)
        VALUES (%(utm_name)s, %(utm_project)s,
            %(utm_notes)s, %(utm_group)s, %(utm_owner)s,
            %(utm_touched)s, %(utm_enabled)s)
    """,
    get_cohort_data.__query_name__:
    """
        SELECT utm_id, utm_project
        FROM <database>.<table>
        WHERE utm_name = %(utm_name)s
    """,
    get_mw_user_id.__query_name__:
    """
        SELECT user_id
        FROM <database>.user
        WHERE user_name = %(username)s
    """,
    get_cohort_users.__query_name__:
    """
        SELECT ut_user
        FROM <database>.<table>
        WHERE ut_tag = %(tag_id)s
    """,
    get_latest_user_activity.__query_name__:
    """
        SELECT
            rev_user,
            MAX(rev_timestamp)
        FROM <database>.revision
        WHERE rev_user in (<users>)
        GROUP BY 1
    """,
}
