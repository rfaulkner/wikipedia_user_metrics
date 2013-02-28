"""
    This module will be used to define Wikimedia Foundation user metrics. The
    Strategy behavioural pattern(http://en.wikipedia.org/wiki/Strategy_pattern)
    will be used to implement the metrics generation. In general the UserMetric
    type utilizes the process() function attribute to produce an internal list
    of metrics for a specified set of user handles (typically ID but user names
    may also be specified) passed to the method on call. The execution of
    process() produces a nested list that can be accessed via generator with
    an object call to __iter__().

    The class structure is generally as follows: ::

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


    These metrics will be used to support experimentation and measurement
    at the Wikimedia Foundation.  The guidelines for this development may
    be found at https://meta.wikimedia.org/wiki/Research:Metrics.

"""

__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging

import user_metrics.etl.data_loader as dl
from collections import namedtuple
from user_metrics.metrics.users import USER_METRIC_PERIOD_TYPE


def pre_metrics_init(init_f):
    """ Decorator function for subclassed metrics __init__ """
    def wrapper(self, **kwargs):
        # Add params from base class
        self.append_params(UserMetric)
        self.apply_default_kwargs(kwargs, 'init')

        # Call init
        init_f(self, **kwargs)

    return wrapper

# Define aggregator processing methods, method attributes, and namedtuple
# class for packaging aggregate data

# Respectively:
#
# 1. flag attribute for a type of metric aggregation methods
# 2. header attribute for a type of metric aggregation methods
# 3. name attribute for a type of metric aggregation methods
# 4. keyword arg attribute for a type of metric aggregation methods
METRIC_AGG_METHOD_FLAG = 'metric_agg_flag'
METRIC_AGG_METHOD_HEAD = 'metric_agg_head'
METRIC_AGG_METHOD_NAME = 'metric_agg_name'
METRIC_AGG_METHOD_KWARGS = 'metric_agg_kwargs'

# Class for storing aggregate data
aggregate_data_class = namedtuple("AggregateData", "header data")


def aggregator(agg_method, metric, data_header):
    """ Method for wrapping and executing aggregated data """

    if hasattr(agg_method, METRIC_AGG_METHOD_FLAG) and getattr(
            agg_method,  METRIC_AGG_METHOD_FLAG):
        # These are metric specific aggregators.  The method must also define
        # the header.
        agg_header = getattr(agg_method, METRIC_AGG_METHOD_HEAD) if hasattr(
            agg_method, METRIC_AGG_METHOD_HEAD) else 'No header specified.'

        kwargs = getattr(agg_method, METRIC_AGG_METHOD_KWARGS) if hasattr(
            agg_method, METRIC_AGG_METHOD_KWARGS) else {}

        data = [getattr(agg_method, METRIC_AGG_METHOD_NAME)] + agg_method(
            metric, **kwargs)
    else:
        # Generic aggregators that are metric agnostic
        agg_header = ['type'] + [
            data_header[i] for i in metric._agg_indices[agg_method.__name__]]
        data = [agg_method.__name__] + agg_method(metric.__iter__(),
                                                  metric._agg_indices[
                                                  agg_method.__name__])
    return aggregate_data_class(agg_header, data)


class UserMetricError(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Unable to process results using "
                               "strategy."):
        Exception.__init__(self, message)


class UserMetric(object):

    ALL_NAMESPACES = 'all_namespaces'
    DATETIME_STR_FORMAT = "%Y%m%d%H%M%S"

    DEFAULT_DATE_START = '20101025080000'
    DEFAULT_DATE_END = '20110101000000'

    # Default number of days for a metric computation
    DEFAULT_DATA_RANGE = 14

    _data_model_meta = dict()
    _agg_indices = dict()

    # Structure that defines parameters for UserMetric class
    _param_types = {
        'init': {
            'datetime_start': [str, 'Earliest date metric '
                                               'is measured.',
                               DEFAULT_DATE_START],
            'datetime_end': [str, 'Latest date metric is measured.',
                             DEFAULT_DATE_END],
            't': [int, 'Hours over which to measure metric.', 24],
            'period_type': [int, 'Defines the type of '
                                                       'period over which '
                                                       'user metrics are '
                                                       'measured.',
                            USER_METRIC_PERIOD_TYPE.REGISTRATION],
            'project': [str, 'The project (language) being inspected.',
                        'enwiki'],
            'namespace': [list, 'The namespace over which the '
                                     'metric is computed.', 0],
        },
        'process': {}
    }

    def apply_default_kwargs(self, kwargs, arg_type):
        """ Apply parameter defaults where necessary """
        if hasattr(kwargs, '__iter__') and arg_type in self._param_types:
            for k in self._param_types[arg_type]:
                if not k in kwargs or not kwargs[k]:
                    kwargs[k] = self._param_types[arg_type][k][2]

    def __init__(self, **kwargs):

        # Stores results of a process request
        self._results = list()

        for att in self._param_types['init']:
            if not att in kwargs:
                setattr(self, att, att[2])
            else:
                setattr(self, att, kwargs[att])

        # Initialize namespace attribute
        if not self.namespace == self.ALL_NAMESPACES:
            if not hasattr(self.namespace, '__iter__'):
                self.namespace = {self.namespace}
            else:
                self.namespace = set(self.namespace)
            self.namespace = list(self.namespace)

    def __str__(self):
        return "\n".join([str(self.__class__),
                          str(self.namespace),
                          self.project])

    def __iter__(self):
        return (r for r in self._results)

    def append_params(self, class_ref):
        """ Append params from class reference """
        if hasattr(class_ref, '_param_types'):
            for k, v in class_ref._param_types['init'].iteritems():
                self.__class__._param_types['init'][k] = v
            for k, v in class_ref._param_types['process'].iteritems():
                self.__class__._param_types['process'][k] = v

    @classmethod
    def _construct_data_point(cls):
        return namedtuple(cls.__name__, cls.header())

    @classmethod
    def _format_namespace(cls, namespace):
        # format the namespace condition
        ns_cond = ''
        if hasattr(namespace, '__iter__'):
            if len(namespace) == 1:
                ns_cond = 'page_namespace = ' + str(namespace.pop())
            else:
                ns_cond = 'page_namespace in (' + ",".join(dl.DataLoader().
                          cast_elems_to_string(list(namespace))) + ')'
        return ns_cond

    @staticmethod
    def header():
        raise NotImplementedError()

    @staticmethod
    def pre_process_users(proc_func):
        def wrapper(self, users, **kwargs):

            # Duck-type the "cohort" ref for a ID generating interface
            # see src/metrics/users.py
            if hasattr(users, 'get_users'):
                logging.info(__name__ + ':: Calling get_users() ...')
                users = [u for u in users.get_users(self._start_ts_,
                                                    self._end_ts_)]

            # If users are empty flag an error
            if not users:
                raise Exception('No users to pass to process method.')

            return proc_func(self, users, **kwargs)
        return wrapper

    def process(self, users, **kwargs):
        raise NotImplementedError()
