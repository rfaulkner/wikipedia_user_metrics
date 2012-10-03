"""

This module will be used to define WMF metrics.  The Template Method behavioural pattern (http://en.wikipedia.org/wiki/Template_method_pattern) will
be used to implement the metrics generation.  For example: ::

    class Metric(object):

        def __init__(self):
            # initialize base metric

            return

        def process(self):
            # base metric implementation

            return metric_value


    class DerivedMetric(Metric):

        def __init__(self):
            super(DerivedMetric, self)

            # initialize derived metric

            return

        def process(self):
            # derived metric implementation

            return metric_value


These metrics will be used to support experimentation and measurement at the Wikimedia Foundation.  The guidelines for this development may be found at
https://meta.wikimedia.org/wiki/Research:Metrics.

"""

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

import libraries.etl.DataLoader as DL
import libraries.etl.TimestampProcessor as TP
import datetime
import MySQLdb
import sys
import logging
from dateutil.parser import *

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class UserMetric(object):

    def __init__(self,
                 datasource=None,
                 project='enwiki',
                 namespace=0,
                 **kwargs):

        if not(isinstance(datasource, DL.DataLoader)):
            self._datasource_ = DL.DataLoader(db='slave')
        else:
            self._datasource_ = datasource

        self._namespace_ = namespace
        self._project_ = project


    def get_timestamp(self, ts_representation):
        """
            Helper method.  Takes a representation of a date object (String or datetime.datetime object) and formats
            as a timestamp: "YYYY-MM-DD HH:II:SS"

            - Parameters:
                - *date_representation* - String or datetime.  A formatted timestamp representation

            - Return:
                - String.  Timestamp derived from argument in format "YYYY-MM-DD HH:II:SS".
        """

        try:
            if isinstance(ts_representation, datetime.datetime):
                ts = str(ts_representation)[:19]
            elif isinstance(ts_representation, str):
                ts = str(parse(ts_representation))
            else:
                raise Exception()

            ts = TP.timestamp_convert_format(ts,2,1)

            return ts
        except:
            logging.info('Could not parse datetime: %s' % str(ts_representation))
            return None


    def escape_var(self, var):
        """
            Escapes either elements of a list (recursively visiting elements) or a single variable.  The variable
            is cast to string before being escaped.

            - Parameters:
                - **var**: List or string.  Variable or list (potentially nested) of variables to be escaped.

            - Return:
                - List or string.  escaped elements.
        """

        # If the input is a list recursively call on elements
        # TODO: potentailly extend for dictionaries
        if isinstance(var, list):
            escaped_var = list()
            for elem in var:
                escaped_var.append(self.escape_var(elem))
            return escaped_var
        else:
            return MySQLdb._mysql.escape_string(str(var))


    def process(self, user_handle, is_id=True):
        """

        """
        return 0