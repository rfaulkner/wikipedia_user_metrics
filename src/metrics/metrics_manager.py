
"""
    This module defines a set of methods useful in handling series of metrics objects to build more complex results.
"""

__author__ = "ryan faulkner"
__date__ = "12/28/2012"
__license__ = "GPL (version 2 or later)"

import re
import src.metrics.user_metric as um
import src.metrics.threshold as th
import src.metrics.blocks as b
import src.metrics.bytes_added as ba
import src.metrics.survival as sv
import src.metrics.revert_rate as rr
import src.metrics.time_to_threshold as ttt
import src.metrics.edit_rate as er
import src.etl.data_loader as dl

metric_dict = {
    'threshold' : th.Threshold,
    'survival' : sv.Survival,
    'revert' : rr.RevertRate,
    'bytes_added' : ba.BytesAdded,
    'blocks' : b.Blocks,
    'time_to_threshold' : ttt.TimeToThreshold,
    'edit_rate' : er.EditRate,
    }

def get_metric_names(): return metric_dict.keys()
def get_param_types(metric_handle): return metric_dict[metric_handle]()._param_types

def process_data_request(metric_obj, users, aggregator_func=None, time_series=False, field_indices=None, **kwargs):

    # Initialize the results
    results = dict()
    results['header'] = " ".join(metric_obj.header())
    for key in metric_obj.__dict__:
        if re.search(r'_.*_', key):
            results[str(key[1:-1])] = str(metric_obj.__dict__[key])
    results['metric'] = dict()

    # Compute the metric
    metric_obj.process(users, num_threads=20, rev_threads=50, **kwargs)
    f = dl.DataLoader().cast_elems_to_string
    if aggregator_func:
        r = um.aggregator(aggregator_func, metric_obj, metric_obj.header(), field_indices)
        results['metric'][r.data[0]] = " ".join(f(r.data[1:]))
        results['header'] = " ".join(f(r.header))
    else:
        for m in metric_obj.__iter__():
            results['metric'][m[0]] = " ".join(dl.DataLoader().cast_elems_to_string(m[1:]))

    return results