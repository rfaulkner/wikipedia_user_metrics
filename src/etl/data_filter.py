"""
    The DataFilters module provides a way to filter data to mutate dictionaries.  Each filter contains an execute( method that
"""

__author__ = "Ryan Faulkner"
__date__ = "July 5th, 2011"
__license__ = "GPL (version 2 or later)"

import sys
import logging
import heapq
import data_loader as dl

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def filter_bots(eligible_users):
    """
        Filter bots based on bot table.  TODO: This can be sped up by sorting lists first.

        Parameters:
            - **users** - list of user names to be matched on the bot REGEX

        Return:
            - List(string).  Filtered results.
    """
    BOT_TABLE = 'halfak.bot'
    conn = dl.Connector(instance='slave-2')
    results = [str(x[0]) for x in conn.execute_SQL('select user_id from %(table)s' % { 'table' : BOT_TABLE})]
    conn.close_db()

    return filter(lambda x: str(x) not in results, eligible_users)

class DataFilter(object):
    """ BASE CLASS for filters.  The interface defines the filter method which is called. """

    def __init__(self, **kwargs):
        """ Perform initialization boilerplate for all classes """
        # The mutable object will contain the data structures on which the filter will operate
        for key in kwargs:
            if key == 'mutable_obj':
                self._mutable_obj_ = kwargs[key]

    def execute(self, **kwargs):
        """ Execution method.  The base class simply performs the logging. """

        return