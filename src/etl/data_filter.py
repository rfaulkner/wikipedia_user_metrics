"""

    The DataFilters module provides a way to filter data to mutate dictionaries.  Each filter contains an execute( method that

"""

__author__ = "Ryan Faulkner"
__date__ = "July 5th, 2011"
__license__ = "GPL (version 2 or later)"

import sys
import logging

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class DataFilter(object):
    """
        BASE CLASS for filters.  The interface defines the filter method which is called
    """


    def __init__(self, **kwargs):
        """
            Perform initialization boilerplate for all classes
        """

        # logging.info('Creating filter ' + self.__str__())

        # The mutable object will contain the data structures on which the filter will operate
        for key in kwargs:
            if key == 'mutable_obj':
                self._mutable_obj_ = kwargs[key]


    def execute(self, **kwargs):
        """
            Execution method.  The base class simply performs the logging.
        """

        return