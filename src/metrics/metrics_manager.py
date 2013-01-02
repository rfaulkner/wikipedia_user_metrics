
"""
    This module defines a set of methods useful in handling series of metrics objects to build more complex results.
"""

__author__ = "ryan faulkner"
__date__ = "12/28/2012"
__license__ = "GPL (version 2 or later)"

import re
from dateutil.parser import parse as date_parse

import src.metrics.user_metric as um
import src.metrics.threshold as th
import src.metrics.blocks as b
import src.metrics.bytes_added as ba
import src.metrics.survival as sv
import src.metrics.revert_rate as rr
import src.metrics.time_to_threshold as ttt
import src.metrics.edit_rate as er
import src.etl.data_loader as dl
import src.etl.aggregator as agg
import src.etl.time_series_process_methods as tspm

INTERVALS_PER_THREAD = 10
MAX_THREADS = 10

metric_dict = {
    'threshold' : th.Threshold,
    'survival' : sv.Survival,
    'revert' : rr.RevertRate,
    'bytes_added' : ba.BytesAdded,
    'blocks' : b.Blocks,
    'time_to_threshold' : ttt.TimeToThreshold,
    'edit_rate' : er.EditRate,
    }

aggregator_dict = {
    'sum+bytes_added' : (agg.list_sum_indices,
                         ba.BytesAdded._data_model_meta['float_fields'] + ba.BytesAdded._data_model_meta['integer_fields']),
    'average+threshold' : (th.threshold_editors_agg, []),
    'average+revert' : (rr.reverted_revs_agg, []),
    }

def get_metric_names(): return metric_dict.keys()
def get_param_types(metric_handle): return metric_dict[metric_handle]()._param_types
def get_agg_key(agg_handle, metric_handle): return '+'.join([agg_handle, metric_handle])

def process_data_request(metric_handle, users, agg_handle='', **kwargs):

    # Initialize the results
    results = dict()
    metric_class = metric_dict[metric_handle]
    metric_obj = metric_class(**kwargs)
    results['header'] = " ".join(metric_obj.header())
    for key in metric_obj.__dict__:
        if re.search(r'_.*_', key):
            results[str(key[1:-1])] = str(metric_obj.__dict__[key])
    results['metric'] = dict()

    # Get the aggregator if there is one
    aggregator_func = None
    field_indices = None

    aggregator_key = get_agg_key(agg_handle, metric_handle)
    if aggregator_key in aggregator_dict.keys():
        aggregator_func = aggregator_dict[aggregator_key][0]
        field_indices = aggregator_dict[aggregator_key][1]

    time_series = True if 'time_series' in kwargs else False

    # Compute the metric
    metric_obj.process(users, num_threads=20, rev_threads=50, **kwargs)
    f = dl.DataLoader().cast_elems_to_string
    if aggregator_func:

        if time_series:

            start = um.UserMetric._get_timestamp(kwargs['date_start'])
            end = um.UserMetric._get_timestamp(kwargs['date_end'])
            interval = int(kwargs['interval'])      # interval length in hours

            total_intervals = (date_parse(end) - date_parse(start)).total_seconds() / (3600 * interval)
            num_threads = min(MAX_THREADS, int(total_intervals / INTERVALS_PER_THREAD))


            out = tspm.build_time_series(start, end, interval, metric_class, aggregator_func, users,
                num_threads=num_threads, metric_threads='{"num_threads" : 20, "rev_threads" : 50}', log=True)

            for row in out:
                results['metric'][row[0] + ' - ' + row[1]] = " ".join(dl.DataLoader().cast_elems_to_string(row[3:]))
        else:
            r = um.aggregator(aggregator_func, metric_obj, metric_obj.header())
            results['metric'][r.data[0]] = " ".join(f(r.data[1:]))
            results['header'] = " ".join(f(r.header))
    else:
        for m in metric_obj.__iter__():
            results['metric'][m[0]] = " ".join(dl.DataLoader().cast_elems_to_string(m[1:]))

    return results