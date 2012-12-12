
"""
    This module contains methods that provide functionality for aggregating metrics data
"""

__author__ = "ryan faulkner"
__date__ = "12/12/2012"
__license__ = "GPL (version 2 or later)"

import src.metrics.threshold as th

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
    return total, pos

class AggregatorException(Exception): pass

