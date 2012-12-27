
"""
    This module contains methods that provide functionality for aggregating metrics data
"""

__author__ = "ryan faulkner"
__date__ = "12/12/2012"
__license__ = "GPL (version 2 or later)"

import src.metrics.threshold as th
import src.metrics.revert_rate as rr
from itertools import izip
from numpy import array

def decorator_builder(header):
    """ Decorator method to annotate aggregation methods to ensure the correct data model is exposed by """
    def eval_data_model(f):
        def wrapper(metric):
            if hasattr(metric,'header'):
                header_arg = metric.header()
                if all(header_arg[i] == header[i] for i in range(len(header)-1)):
                    return f(metric)
            else:
                raise AggregatorException('This aggregator (%s) does not operate on this data type.' % f.__name__)
        return wrapper
    return eval_data_model

@decorator_builder(th.Threshold.header())
def threshold_editors_agg(metric):
    """ Computes the fraction of editors reaching a threshold """
    total=0
    pos=0
    for r in metric.__iter__():
        try:
            if r[1]: pos+=1
            total+=1
        except IndexError: continue
        except TypeError: continue
    if total:
        return total, pos, float(pos) / total
    else:
        return total, pos, 0.0

@decorator_builder(rr.RevertRate.header())
def reverted_revs_agg(metric):
    """ Computes revert metrics on a user set """
    total_revs = 0
    weighted_rate = 0.0
    total_editors = 0
    reverted_editors = 0
    for r in metric.__iter__():
        try:
            reverted_revs = int(r[2])
            total_editors += 1
            if reverted_revs: reverted_editors += 1
            total_revs += reverted_revs
            weighted_rate += reverted_revs * float(r[1])
        except IndexError: continue
        except TypeError: continue
    if total_revs:
        weighted_rate /= total_revs
    else:
        weighted_rate = 0.0
    return total_revs, weighted_rate, total_editors, reverted_editors

def list_sum_by_group(l, group_index):
    """
        Sums the elements of list keyed on `key_index`.  The elements must be summable (i.e. e1 + e2 is allowed
        for all e1 and e2).  All elements outside of key are summed on matching keys.

        Returns: <list of summed and keyed elements>

        e.g.
        >>> l = [[2,1],[1,4],[2,2]]
        >>> list_sum_by_group(l,0)
        [[1,4], [2,3]]
    """
    d=dict()
    for i in l:
        summables = i[:group_index] + i[group_index+1:]
        if d.has_key(i[group_index]):
            d[i[group_index]] = map(sum, izip(summables,d[i[group_index]]))
        else:
            d[i[group_index]] = summables
    return [d[k][:group_index] + [k] + d[k][group_index:] for k in d]


def list_average_by_group(l, group_index):
    """
        Computes the average of the elements of list keyed on `key_index`.  The elements must be summable
        (i.e. e1 + e2 is allowed for all e1 and e2).  All elements outside of key are summed on matching keys.
        This duplicates the code of `list_sum_by_group` since it needs to compute counts in the loop also.

        Returns: <list of averaged and keyed elements>

        e.g.
        >>> l = [[2,1],[1,4],[2,2]]
        >>> list_average(l,0)
        [[1, 4.0], [2, 1.5]]
    """
    d=dict()
    counts=dict()
    for i in l:
        summables = i[:group_index] + i[group_index+1:]
        if d.has_key(i[group_index]):
            d[i[group_index]] = map(sum, izip(summables,d[i[group_index]]))
            counts[i[group_index]] += 1
        else:
            d[i[group_index]] = summables
            counts[i[group_index]] = 1
    for k in counts: d[k] = list(array(d[k]) / float(counts[k]))     # use numpy array perform list operation
    return [d[k][:group_index] + [k] + d[k][group_index:] for k in d]

class AggregatorException(Exception): pass

