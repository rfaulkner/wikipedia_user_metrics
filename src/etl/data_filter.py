"""
    The DataFilters module provides a way to filter data to mutate dictionaries.  Each filter contains an execute( method that
"""

__author__ = "Ryan Faulkner"
__date__ = "July 5th, 2011"
__license__ = "GPL (version 2 or later)"

import sys
import logging
import re
import data_loader as dl

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def filter_bots(user_ids, project='enwiki'):
    """
        Filter bots from a user list.

        Parameters:
            - **users** - list of user names to be matched on the bot REGEX

        Return:
            - List(string).  Filtered results.
    """

    BOT_REGEX = r'[Bb][Oo][Tt]'

    # Extract user names from the db
    d = dl.Handle(db='slave')
    user_id_str = d.format_comma_separated_list(user_ids, include_quotes=False)
    d._cur_.execute('select user_id, user_name from %(project)s.user where user_id in (%(user_list)s)' % {
        'project' : project,
        'user_list' : user_id_str})

    # Select user ids from the input not corresponding to bots
    return filter(lambda x: not x == -1,
        map(lambda x: str(x[0]) if not(re.search(BOT_REGEX, str(x[1]))) else -1, d._cur_))

class DataFilter(object):
    """ BASE CLASS for filters.  The interface defines the filter method which is called. """

    def __init__(self, **kwargs):
        """ Perform initialization boilerplate for all classes """

        # The mutable object will contain the data structures on which the filter will operate
        for key in kwargs:
            if key == 'mutable_obj':
                self._mutable_obj_ = kwargs[key]

    def execute(self, **kwargs):
        """
            Execution method.  The base class simply performs the logging.
        """

        return