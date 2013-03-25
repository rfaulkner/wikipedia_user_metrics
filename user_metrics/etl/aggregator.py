
"""
    This module contains methods that provide functionality for aggregating
    metrics data.  This is essential to user metrics computations as often
    it is necessary to work with some aggregate form of large user data sets.

    Registering an aggregator with a UserMetric derived class
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Aggregators are "registered" with UserMetric classes in order that they
    may be called at runtime.  The API documentation contains instructions
    on how these aggregators may be bound to queries.  For the purposes of
    simply defining an aggregator for a metric however, the example of the
    "boolean_rate" aggregator method on the "Threshold" metric serves to
    illustrate how this is accomplished independent of the API::

        # Build "rate" decorator
        threshold_editors_agg = boolean_rate
        threshold_editors_agg = decorator_builder(Threshold.header())(
            threshold_editors_agg)

        setattr(threshold_editors_agg, um.METRIC_AGG_METHOD_FLAG, True)
        setattr(threshold_editors_agg, um.METRIC_AGG_METHOD_NAME,
            'threshold_editors_agg')
        setattr(threshold_editors_agg, um.METRIC_AGG_METHOD_HEAD,
        ['total_users', 'threshold_reached','rate'])
        setattr(threshold_editors_agg, um.METRIC_AGG_METHOD_KWARGS, {
        'val_idx' : 1})

    The attributes set above are defined in the user_mertric module::

        # 1. flag attribute for a type of metric aggregation methods
        # 2. header attribute for a type of metric aggregation methods
        # 3. name attribute for a type of metric aggregation methods
        # 4. keyword arg attribute for a type of metric aggregation methods
        METRIC_AGG_METHOD_FLAG = 'metric_agg_flag'
        METRIC_AGG_METHOD_HEAD = 'metric_agg_head'
        METRIC_AGG_METHOD_NAME = 'metric_agg_name'
        METRIC_AGG_METHOD_KWARGS = 'metric_agg_kwargs'

    In this way aggregators and metrics can be combined freely.  New aggregator
    methods can be written to perform different types of aggregation.

    Aggregator Methods
    ~~~~~~~~~~~~~~~~~~
"""

__author__ = "ryan faulkner"
__date__ = "12/12/2012"
__license__ = "GPL (version 2 or later)"

from types import FloatType
from collections import namedtuple
from itertools import izip
from numpy import array, transpose
from user_metrics.metrics.user_metric import METRIC_AGG_METHOD_FLAG, \
    METRIC_AGG_METHOD_HEAD, \
    METRIC_AGG_METHOD_KWARGS, \
    METRIC_AGG_METHOD_NAME

# Type used to carry aggregator meta data
AggregatorMeta = namedtuple('AggregatorMeta', 'field_name index op')


def decorator_builder(header):
    """
        Decorator method to annotate aggregation methods to ensure the correct
        data model is exposed by::

            metric_agg = decorator_builder(Metric.header())(metric_agg)
    """
    def eval_data_model(f):
        def wrapper(metric, **kwargs):
            if hasattr(metric, 'header'):
                header_arg = metric.header()
                if all(
                        header_arg[i] == header[i]
                        for i in range(len(header)-1)):
                    return f(metric, **kwargs)
            else:
                raise AggregatorError('This aggregator (%s) does not operate '
                                      'on this data type.' % f.__name__)
        return wrapper
    return eval_data_model


def list_sum_indices(l, indices):
    """
        Sums the elements of list indicated by numeric list `indices`.  The
        elements must be summable (i.e. e1 + e2 is allowed for all e1 and e2)

            Returns: <list of summed elements>

            e.g.
            >>> l = [['1',1,50],['2',4,1],['3',2,6]]
            >>> list_sum_indices(l,[1,2])
            [7, 57]
    """
    return list(reduce(lambda x, y: x+y,
                       [array([elem.__getitem__(i) for i in indices])
                        for elem in l]))


def list_sum_by_group(l, group_index):
    """
        Sums the elements of list keyed on `key_index`. The elements must be
        summable (i.e. e1 + e2 is allowed for all e1 and e2).  All elements
        outside of key are summed on matching keys::

            Returns: <list of summed and keyed elements>

            e.g.
            >>> l = [[2,1],[1,4],[2,2]]
            >>> list_sum_by_group(l,0)
            [[1,4], [2,3]]
    """
    d = dict()
    for i in l:
        summables = i[:group_index] + i[group_index+1:]
        if i[group_index] in d:
            d[i[group_index]] = map(sum, izip(summables, d[i[group_index]]))
        else:
            d[i[group_index]] = summables
    return [d[k][:group_index] + [k] + d[k][group_index:] for k in d]


def list_average_by_group(l, group_index):
    """
        Computes the average of the elements of list keyed on `key_index`.
        The elements must be summable (i.e. e1 + e2 is allowed for all e1 and
        e2).  All elements outside of key are summed on matching keys. This
        duplicates the code of `list_sum_by_group` since it needs to compute
        counts in the loop also::

            Returns: <list of averaged and keyed elements>

            e.g.
            >>> l = [[2,1],[1,4],[2,2]]
            >>> list_average(l,0)
            [[1, 4.0], [2, 1.5]]
    """
    d = dict()
    counts = dict()
    for i in l:
        summables = i[:group_index] + i[group_index+1:]
        if i[group_index] in d:
            d[i[group_index]] = map(sum,
                                    izip(summables, d[i[group_index]]))
            counts[i[group_index]] += 1
        else:
            d[i[group_index]] = summables
            counts[i[group_index]] = 1

    # use numpy array perform list operation
    for k in counts:
        d[k] = list(array(d[k]) / float(counts[k]))
    return [d[k][:group_index] + [k] + d[k][group_index:] for k in d]


def boolean_rate(iter, **kwargs):
    """
        Computes the fraction of rows meeting a comparison criteria defined
        by `cmp_method`.  The index containing the value is passed as a kwarg
        (`bool_idx`).

        Useful aggregator for boolean metrics: threshold, survival,
                live_accounts
    """

    def cmp_method_default(x):
        return x > 0

    val_idx = kwargs['val_idx'] if 'val_idx' in kwargs else 1
    cmp_method = kwargs['cmp_method'] if 'cmp_method' in kwargs \
        else cmp_method_default

    total = 0
    pos = 0
    for r in iter.__iter__():
        try:
            if cmp_method(r[val_idx]):
                pos += 1
            total += 1
        except IndexError:
            continue
        except TypeError:
            continue
    if total:
        return [total, pos, float(pos) / total]
    else:
        return [total, pos, 0.0]


def weighted_rate(iter, **kwargs):
    """
        Computes a weighted rate over the elements of the iterator.
    """

    def weight_method_default(x):
        return 1

    weight_idx = kwargs['weight_idx'] if 'weight_idx' in kwargs else 1
    val_idx = kwargs['val_idx'] if 'val_idx' in kwargs else 1
    weight_method = kwargs['weight_method'] if 'cmp_method' in kwargs else \
        weight_method_default

    count = 0
    total_weight = 0.0
    weighted_sum = 0.0
    for r in iter.__iter__():
        try:
            count += 1
            weight = weight_method(r[weight_idx])
            total_weight += r[weight_idx]
            weighted_sum += weight * r[val_idx]
        except IndexError:
            continue
        except TypeError:
            continue
    if count:
        return [count, total_weight, weighted_sum / count]
    else:
        return [count, total_weight, 0.0]


def numpy_op(iter, **kwargs):
    """
        Computes specified numpy op from an iterator exposing a dataset.

            **iter** - assumed to be a UserMetric class with _results defined
            as a list of datapoints
    """

    # Retrieve indices on data for which to compute medians
    agg_meta = kwargs['agg_meta']
    values = list()

    # Convert data points to numpy array
    if hasattr(iter, '_results'):
        results = array(iter._results)
    else:
        results = list()
        for i in iter:
            results.append(i)
        results = array(iter._results)

    # Transpose the array and convert it's elements to Python FloatType
    results = transpose(results)
    results = results.astype(FloatType)

    # Compute the median of each specified data index
    for agg_meta_obj in agg_meta:
        if not hasattr(agg_meta_obj, 'op') or \
                not hasattr(agg_meta_obj, 'index'):
            raise AggregatorError(__name__ + ':: Use AggregatorMeta object to '
                                             'pass aggregator meta data.')
        values.append(agg_meta_obj.op(results[agg_meta_obj.index, :]))
    return values


def build_numpy_op_agg(agg_meta_list, metric_header, method_handle):
    """
        Builder method for ``numpy_op`` aggregator.
    """
    agg_method = numpy_op
    agg_method = decorator_builder(metric_header)(agg_method)
    agg_meta_header = [o.field_name for o in agg_meta_list]

    setattr(agg_method, METRIC_AGG_METHOD_FLAG, True)
    setattr(agg_method, METRIC_AGG_METHOD_NAME, method_handle)
    setattr(agg_method, METRIC_AGG_METHOD_HEAD, agg_meta_header)
    setattr(agg_method, METRIC_AGG_METHOD_KWARGS,
            {
                'agg_meta': agg_meta_list
            }
            )
    return agg_method


def build_agg_meta(op_list, field_prefix_names):
    """
        Builder helper method for building module aggregators via
        ``build_numpy_op_agg``.
    """
    return [AggregatorMeta(name + op.__name__, index, op)
            for name, index in field_prefix_names.iteritems()
            for op in op_list]


class AggregatorError(Exception):
    """ Basic exception class for aggregators """
    def __init__(self, message="Aggregation error."):
        Exception.__init__(self, message)
