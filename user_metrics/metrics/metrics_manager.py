
"""
    This module defines a set of methods useful in handling series of metrics
    objects to build more complex results.  This generally involves creating
    one or more UserMetric derived objects with passed parameters to service
    a request.  The primary entry point is the ``process_data_request`` method.
    This method coordinates requests for three different top-level request
    types:

    - **Raw requests**.  Output is a set of datapoints that consist of the
      user IDs accompanied by metric results.
    - **Aggregate requests**.  Output is an aggregate of all user results based
      on the type of aggregaion as defined in the aggregator module.
    - **Time series requests**.  Outputs a time series list of data.  For this
      type of request a start and end time must be defined along with an
      interval length.  Further an aggregator must be provided which operates
      on each time interval.

    Also defined are metric types for which requests may be made with
    ``metric_dict``, and the types of aggregators that may be called on metrics
    ``aggregator_dict``, and also the meta data around how many threads may be
    used to process metrics ``USER_THREADS`` and ``REVISION_THREADS``.

    Module definitions
    ~~~~~~~~~~~~~~~~~~
"""

__author__ = "ryan faulkner"
__email__ = "rfaulkner@wikimedia.org"
__date__ = "12/28/2012"
__license__ = "GPL (version 2 or later)"

import re
from collections import OrderedDict
from dateutil.parser import parse as date_parse

import user_metric as um
import threshold as th
from blocks import Blocks, block_rate_agg
from bytes_added import BytesAdded, ba_median_agg, ba_min_agg, ba_max_agg
from survival import Survival, survival_editors_agg
from revert_rate import RevertRate, revert_rate_avg
from time_to_threshold import TimeToThreshold, ttt_avg_agg, ttt_stats_agg
from edit_rate import EditRate, edit_rate_agg, er_stats_agg
from namespace_of_edits import NamespaceEdits, namespace_edits_sum
from live_account import LiveAccount, live_accounts_agg

import user_metrics.etl.data_loader as dl
import user_metrics.etl.aggregator as agg
import user_metrics.etl.time_series_process_methods as tspm

from user_metrics.config import logging, settings

INTERVALS_PER_THREAD = 10
MAX_THREADS = 5

USER_THREADS = settings.__user_thread_max__
REVISION_THREADS = settings.__rev_thread_max__

# Registered metrics types
metric_dict = \
    {
        'threshold': th.Threshold,
        'survival': Survival,
        'revert_rate': RevertRate,
        'bytes_added': BytesAdded,
        'blocks': Blocks,
        'time_to_threshold': TimeToThreshold,
        'edit_rate': EditRate,
        'namespace_edits': NamespaceEdits,
        'live_account': LiveAccount,
    }

# @TODO: let metric types handle this mapping themselves and obsolete this
#            structure
aggregator_dict = \
    {
        'sum+bytes_added': agg.list_sum_indices,
        'sum+edit_rate': agg.list_sum_indices,
        'sum+namespace_edits': namespace_edits_sum,
        'average+threshold': th.threshold_editors_agg,
        'average+survival': survival_editors_agg,
        'average+live_account': live_accounts_agg,
        'average+revert_rate': revert_rate_avg,
        'average+edit_rate': edit_rate_agg,
        'average+time_to_threshold': ttt_avg_agg,
        'median+bytes_added': ba_median_agg,
        'min+bytes_added': ba_min_agg,
        'max+bytes_added': ba_max_agg,
        'dist+edit_rate': er_stats_agg,
        'average+blocks': block_rate_agg,
        'dist+time_to_threshold': ttt_stats_agg,
    }


def get_metric_names():
    """ Returns the names of metric handles as defined by this module """
    return metric_dict.keys()


def get_param_types(metric_handle):
    """ Get the paramters for a given metric handle """
    return metric_dict[metric_handle]()._param_types


def get_agg_key(agg_handle, metric_handle):
    """ Compose the metric dependent aggregator handle """
    try:
        agg_key = '+'.join([agg_handle, metric_handle])
        if agg_key in aggregator_dict:
            return agg_key
        else:
            return ''
    except TypeError:
        return ''


def process_data_request(metric_handle, users, **kwargs):
    """
        Main entry point of the module.  Coordinates a request based on the
        following parameters::

            metric_handle (string) - determines the type of metric object to
            build.  Keys metric_dict.

            users (list) - list of user IDs.

            **kwargs - Keyword arguments may contain a variety of variables.
            Most notably, "aggregator" if the request requires aggregation,
            "time_series" flag indicating a time series request.  The
            remaining kwargs specify metric object parameters.
    """
    # create shorthand method refs
    to_string = dl.DataLoader().cast_elems_to_string

    aggregator = kwargs['aggregator'] if 'aggregator' in kwargs else None
    agg_key = get_agg_key(aggregator, metric_handle) if aggregator else None

    # Initialize the results
    results = OrderedDict()

    metric_class = metric_dict[metric_handle]
    metric_obj = metric_class(**kwargs)

    start = metric_obj.date_start
    end = metric_obj.date_end

    results['header'] = " ".join(metric_obj.header())
    for key in metric_obj.__dict__:
        if re.search(r'_.*_', key):
            results[str(key[1:-1])] = str(metric_obj.__dict__[key])
    results['metric'] = OrderedDict()

    # Parse the aggregator
    aggregator_func = None
    if agg_key in aggregator_dict.keys():
        aggregator_func = aggregator_dict[agg_key]

    # Parse the time series flag
    time_series = True if 'time_series' in kwargs and kwargs['time_series'] \
        else False

    if aggregator_func:
        if time_series:
            # interval length in hours
            interval = int(kwargs['interval'])
            total_intervals = (date_parse(end) - date_parse(start)).\
                total_seconds() / (3600 * interval)
            time_threads = max(1, int(total_intervals / INTERVALS_PER_THREAD))
            time_threads = min(MAX_THREADS, time_threads)

            logging.info('Metrics Manager: Initiating time series for '
                         '%(metric)s with %(agg)s from '
                         '%(start)s to %(end)s.' %
                         {
                         'metric': metric_class.__name__,
                         'agg': aggregator_func.__name__,
                         'start': str(start),
                         'end': str(end),
                         })
            metric_threads = '{"num_threads" : %(user_threads)s, ' + \
                             '"rev_threads" : %(rev_threads)s}' % \
                             {
                             'user_threads': USER_THREADS,
                             'rev_threads': REVISION_THREADS
                             }
            out = tspm.build_time_series(start,
                                         end,
                                         interval,
                                         metric_class,
                                         aggregator_func,
                                         users,
                                         num_threads=time_threads,
                                         metric_threads=metric_threads,
                                         log=True)

            count = 1
            for row in out:
                results['metric'][count] = " ".join(
                    to_string([row[0][:10] + 'T' + row[0][11:13]] + row[3:]))
                count += 1
        else:

            logging.info('Metrics Manager: Initiating aggregator for '
                         '%(metric)s with %(agg)s from '
                         '%(start)s to %(end)s.' %
                         {
                         'metric': metric_class.__name__,
                         'agg': aggregator_func.__name__,
                         'start': str(start),
                         'end': str(end),
                         })
            metric_obj.process(users, num_threads=USER_THREADS,
                               rev_threads=REVISION_THREADS,
                               log_progress=True, **kwargs)
            r = um.aggregator(aggregator_func, metric_obj, metric_obj.header())
            results['metric'][r.data[0]] = " ".join(to_string(r.data[1:]))
            results['header'] = " ".join(to_string(r.header))
    else:

        logging.info('Metrics Manager: Initiating user data for '
                     '%(metric)s from %(start)s to %(end)s.' %
                     {
                     'metric': metric_class.__name__,
                     'start': str(start),
                     'end': str(end),
                     })
        metric_obj.process(users, num_threads=USER_THREADS,
                           rev_threads=REVISION_THREADS,
                           log_progress=True, **kwargs)
        for m in metric_obj.__iter__():
            results['metric'][m[0]] = " ".join(to_string(m[1:]))

    return results
