
"""
    This module defines a set of methods useful in handling series of metrics objects to build more complex results.
"""

__author__ = "ryan faulkner"
__date__ = "12/28/2012"
__license__ = "GPL (version 2 or later)"

import re
from dateutil.parser import parse as date_parse

import user_metric as um
import threshold as th
from blocks import Blocks
from bytes_added import BytesAdded
from survival import Survival, survival_editors_agg
import revert_rate as rr
from time_to_threshold import TimeToThreshold
from edit_rate import EditRate
from namespace_of_edits import NamespaceEdits, namespace_edits_sum
from live_account import LiveAccount, live_accounts_agg

import src.etl.data_loader as dl
import src.etl.aggregator as agg
import src.etl.time_series_process_methods as tspm

from config import logging

INTERVALS_PER_THREAD = 10
MAX_THREADS = 10

# Registered metrics types
metric_dict = {
    'threshold' : th.Threshold,
    'survival' : Survival,
    'revert_rate' : rr.RevertRate,
    'bytes_added' : BytesAdded,
    'blocks' : Blocks,
    'time_to_threshold' : TimeToThreshold,
    'edit_rate' : EditRate,
    'namespace_edits' : NamespaceEdits,
    'live_account' : LiveAccount,
    }

aggregator_dict = {
    'sum+bytes_added' : agg.list_sum_indices,
    'sum+edit_rate' : agg.list_sum_indices,
    'sum+namespace_edits' : namespace_edits_sum,
    'average+threshold' : th.threshold_editors_agg,
    'average+survival' : survival_editors_agg,
    'average+live_account' : live_accounts_agg,
    'average+revert' : rr.reverted_revs_agg,
    }

def get_metric_names(): return metric_dict.keys()
def get_param_types(metric_handle): return metric_dict[metric_handle]()._param_types

def get_agg_key(agg_handle, metric_handle):
    """ Compose the metric dependent aggregator handle """
    try:
        agg_key = '+'.join([agg_handle, metric_handle])
        if aggregator_dict.has_key(agg_key):
            return agg_key
        else:
            return ''
    except TypeError:
        return ''

def process_data_request(metric_handle, users, **kwargs):

    # create shorthand method refs
    to_string = dl.DataLoader().cast_elems_to_string

    aggregator = kwargs['aggregator'] if 'aggregator' in kwargs else None
    agg_key = get_agg_key(aggregator, metric_handle) if aggregator else None

    # Initialize the results
    results = dict()

    metric_class = metric_dict[metric_handle]
    metric_obj = metric_class(**kwargs)

    start = metric_obj.date_start
    end = metric_obj.date_end

    results['header'] = " ".join(metric_obj.header())
    for key in metric_obj.__dict__:
        if re.search(r'_.*_', key):
            results[str(key[1:-1])] = str(metric_obj.__dict__[key])
    results['metric'] = dict()

    # Parse the aggregator
    aggregator_func = None
    if agg_key in aggregator_dict.keys():
        aggregator_func = aggregator_dict[agg_key]

    # Parse the time series flag
    time_series = True if 'time_series' in kwargs and kwargs['time_series'] else False

    if aggregator_func:
        if time_series:
            interval = int(kwargs['interval'])      # interval length in hours
            total_intervals = (date_parse(end) - date_parse(start)).total_seconds() / (3600 * interval)
            num_threads = min(MAX_THREADS, int(total_intervals / INTERVALS_PER_THREAD))

            logging.info('Metrics Manager: Initiating time series for %(metric)s with %(agg)s from '
                         '%(start)s to %(end)s.' % {
                'metric' : metric_class.__name__,
                'agg' : aggregator_func.__name__,
                'start' : str(start),
                'end' : str(end),
            })
            out = tspm.build_time_series(start, end, interval, metric_class, aggregator_func, users,
                num_threads=num_threads, metric_threads='{"num_threads" : 20, "rev_threads" : 50}', log=True)

            for row in out:
                results['metric'][row[0] + ' - ' + row[1]] = " ".join(to_string(row[3:]))
        else:

            logging.info('Metrics Manager: Initiating aggregator for %(metric)s with %(agg)s from '
                         '%(start)s to %(end)s.' % {
                'metric' : metric_class.__name__,
                'agg' : aggregator_func.__name__,
                'start' : str(start),
                'end' : str(end),
                })

            metric_obj.process(users, num_threads=30, rev_threads=50, **kwargs)
            r = um.aggregator(aggregator_func, metric_obj, metric_obj.header())
            results['metric'][r.data[0]] = " ".join(to_string(r.data[1:]))
            results['header'] = " ".join(to_string(r.header))
    else:

        logging.info('Metrics Manager: Initiating user data for %(metric)s from '
                     '%(start)s to %(end)s.' % {
            'metric' : metric_class.__name__,
            'start' : str(start),
            'end' : str(end),
            })
        metric_obj.process(users, num_threads=30, rev_threads=50, log_progress=True, **kwargs)
        for m in metric_obj.__iter__():
            results['metric'][m[0]] = " ".join(to_string(m[1:]))

    return results