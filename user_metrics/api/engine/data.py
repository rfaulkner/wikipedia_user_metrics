"""
    This module handles API functionality involved in extracting custom data
    for API specific requests along with ETL and data storage operations.

    Data Storage and Retrieval
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    This portion of the API engine is concerned with extracting and storing
    data at runtime to service API requests.  This includes all of the
    interaction with the ``usertags`` and ``usertags_meta`` SQL tables that
    define user cohorts.  The following methods are involved with extracting
    this data::

        get_users(cohort_expr)
        get_cohort_id(utm_name)
        get_cohort_refresh_datetime(utm_id)

    The other portion of data storage and retrieval is concerned with providing
    functionality that enables responses to be cached.  Request responses are
    currently cached in the ``api_data`` OrderedDict_ defined in the run
    module.  This object stores responses in a nested fashion using URL request
    variables and their corresponding values.  For example, the url
    ``http://metrics-api.wikimedia.org/cohorts/e3_ob2b/revert_rate?t=10000``
    maps to::

        api_data['cohort_expr <==> e3_ob2b']['metric <==> revert_rate']
        ['start <==> xx']['start <==> yy']['t <==> 10000']

    The list of key values for a given request is referred to as it's "key
    signature".  The order of parameters is perserved.

    The ``get_data`` method requires a reference to ``api_data``. Given this
    reference and a RequestMeta object the method attempts to find an entry
    for the request if one exists.  The ``set_data`` method does much the
    same operation but performs storage into the hash reference passed.
    The method ``get_url_from_keys`` builds URLs from nested hash references
    using the key list and ``build_key_tree`` recursively builds a tree
    representation of all of the key paths in the hash reference.

    .. _OrderedDict: http://docs.python.org/2/library/collections.html

"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
    }
__date__ = "2012-01-11"
__license__ = "GPL (version 2 or later)"

from flask import escape, redirect, url_for
from datetime import datetime
from re import search
from collections import OrderedDict
from hashlib import sha1

import user_metrics.etl.data_loader as dl
from user_metrics.config import logging
from user_metrics.api.engine import COHORT_REGEX, parse_cohorts, \
    HASH_KEY_DELIMETER
from user_metrics.api.engine.request_meta import REQUEST_META_QUERY_STR,\
    REQUEST_META_BASE, DATETIME_STR_FORMAT
from user_metrics.api import MetricsAPIError


def get_users(cohort_expr):
    """ get users from cohort """

    if search(COHORT_REGEX, cohort_expr):
        logging.info(__name__ + '::Processing cohort by expression.')
        users = [user for user in parse_cohorts(cohort_expr)]
    else:
        logging.info(__name__ + '::Processing cohort by tag name.')
        conn = dl.Connector(instance='slave')
        try:
            conn._cur_.execute('select utm_id from usertags_meta '
                               'WHERE utm_name = "%s"' % str(cohort_expr))
            res = conn._cur_.fetchone()[0]
            conn._cur_.execute('select ut_user from usertags '
                               'WHERE ut_tag = "%s"' % res)
        except IndexError:
            redirect(url_for('cohorts'))
        users = [r[0] for r in conn._cur_]
        del conn
    return users


def get_cohort_id(utm_name):
    """ Pull cohort ids from cohort handles """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('SELECT utm_id FROM usertags_meta '
                       'WHERE utm_name = "%s"' % str(escape(utm_name)))

    utm_id = None
    try:
        utm_id = conn._cur_.fetchone()[0]
    except ValueError:
        pass

    # Ensure the field was retrieved
    if not utm_id:
        logging.error(__name__ + '::Missing utm_id for cohort %s.' %
                                 str(utm_name))
        utm_id = -1

    del conn
    return utm_id


def get_cohort_refresh_datetime(utm_id):
    """
        Get the latest refresh datetime of a cohort.  Returns current time
        formatted as a string if the field is not found.
    """
    conn = dl.Connector(instance='slave')
    conn._cur_.execute('SELECT utm_touched FROM usertags_meta '
                       'WHERE utm_id = %s' % str(escape(utm_id)))

    utm_touched = None
    try:
        utm_touched = conn._cur_.fetchone()[0]
    except ValueError:
        pass

    # Ensure the field was retrieved
    if not utm_touched:
        logging.error(__name__ + '::Missing utm_touched for cohort %s.' %
                                 str(utm_id))
        utm_touched = datetime.now()

    del conn
    return utm_touched.strftime(DATETIME_STR_FORMAT)


def get_data(request_meta, hash_table_ref):
    """ Extract data from the global hash given a request object """

    # Traverse the hash key structure to find data
    # @TODO rather than iterate through REQUEST_META_BASE &
    #   REQUEST_META_QUERY_STR look only at existing attributes

    logging.debug(__name__ + " :: Attempting to pull data for request {0}".
    format(str(request_meta)))

    for key_name in REQUEST_META_BASE + REQUEST_META_QUERY_STR:
        if hasattr(request_meta, key_name) and getattr(request_meta, key_name):
            key = getattr(request_meta, key_name)
        else:
            continue

        full_key = key_name + HASH_KEY_DELIMETER + key
        if hasattr(hash_table_ref, 'has_key') and full_key in hash_table_ref:
            hash_table_ref = hash_table_ref[full_key]
        else:
            return None

    # Ensure that an interface that does not rely on keyed values is returned
    # all data must be in interfaces resembling lists
    if not hasattr(hash_table_ref, '__iter__'):
        return hash_table_ref
    else:
        return None


def set_data(request_meta, data, hash_table_ref):
    """
        Given request meta-data and a dataset create a key path in the global
        hash to store the data
    """
    key_sig = build_key_signature(request_meta)

    if not key_sig:
        logging.error(__name__ + ' :: Could not consruct a key '
                                 'signature from request.')
        return

    logging.debug(__name__ + " :: Adding data to hash @ key signature = {0}".
    format(str(key_sig)))

    # For each key in the key signature add a nested key to the hash
    last_item = key_sig[len(key_sig) - 1]
    for key in key_sig:
        if key != last_item:
            if not (hasattr(hash_table_ref, 'has_key') and
                    key in hash_table_ref and
                    hasattr(hash_table_ref[key], 'has_key')):
                hash_table_ref[key] = OrderedDict()

            hash_table_ref = hash_table_ref[key]
        else:
            hash_table_ref[key] = data


def build_key_signature(request_meta, hash_result=False):
    """
        Given a RequestMeta object contruct a hashkey.

        Parameters
        ~~~~~~~~~~

            request_meta : RequestMeta
                Stores request data.
    """
    key_sig = list()

    # Build the key signature -- These keys must exist
    for key_name in REQUEST_META_BASE:
        key = getattr(request_meta, key_name)
        if key:
            key_sig.append(key_name + HASH_KEY_DELIMETER + key)
        else:
            logging.error(__name__ + ' :: Request must include %s. '
                                     'Cannot set data %s.' %
                                     (key_name, str(request_meta)))
            return ''
    # These keys may optionally exist
    for key_name in REQUEST_META_QUERY_STR:
        if hasattr(request_meta, key_name):
            key = getattr(request_meta, key_name)
            if key:
                key_sig.append(key_name + HASH_KEY_DELIMETER + key)

    if hash_result:
        return sha1(str(key_sig).encode('utf-8')).hexdigest()
    else:
        return key_sig


def get_url_from_keys(keys, path_root):
    """ Compose a url from a set of keys """
    query_str = ''
    for key in keys:
        parts = key.split(HASH_KEY_DELIMETER)
        if parts[0] in REQUEST_META_BASE:
            path_root += '/' + parts[1]
        elif parts[0] in REQUEST_META_QUERY_STR:
            query_str += parts[0] + '=' + parts[1] + '&'

    if not path_root:
        raise MetricsAPIError()
    if query_str:
        url = path_root + '?' + query_str[:-1]
    else:
        url = path_root
    return url


def build_key_tree(nested_dict):
    """ Builds a tree of key values from a nested dict. """
    if hasattr(nested_dict, 'keys'):
        for key in nested_dict.keys():
            yield (key, build_key_tree(nested_dict[key]))
    else:
        yield None


def get_keys_from_tree(tree):
    """
        Depth first traversal - get the key signatures from structure
         produced by ``build_key_tree``.
    """
    key_sigs = list()
    for node in tree:
        stack_trace = [node]
        while stack_trace:
            if stack_trace[-1]:
                ptr = stack_trace[-1][1]
                try:
                    stack_trace.append(ptr.next())
                except StopIteration:
                    # no more children
                    stack_trace.pop()
            else:
                key_sigs.append([elem[0] for elem in stack_trace[:-1]])
                stack_trace.pop()
    return key_sigs