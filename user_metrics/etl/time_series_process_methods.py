
"""
    This module contains custom methods to extract time series data.
"""

__author__ = "ryan faulkner"
__date__ = "12/10/2012"
__license__ = "GPL (version 2 or later)"

import datetime
import time
import os
from copy import deepcopy
from dateutil.parser import parse as date_parse
import operator
import json

import src.metrics.revert_rate as rr
import src.metrics.user_metric as um
from multiprocessing import Process, Queue

from config import logging

# Determines the amount of time to wait before picking completed threads off
# of the queue
PROCESS_SLEEP_TIME      = 5

def _get_timeseries(date_start, date_end, interval):
    """
        Generates a series of timestamps given a start date,
        end date, and interval
    """

    # Ensure the dates are string representations
    date_start = um.UserMetric._get_timestamp(date_start)
    date_end = um.UserMetric._get_timestamp(date_end)

    # ensure that at least two intervals are included in the time series
    if (date_parse(date_end) - date_parse(date_start)).\
       total_seconds() / 3600 < interval:
        raise TimeSeriesException(message="Time series must contain at " \
                                          "least one interval.")

    c = date_parse(date_start) + datetime.timedelta(hours=-interval)
    e = date_parse(date_end)
    while c < e:
        c += datetime.timedelta(hours=interval)
        yield c

def build_time_series(start, end, interval, metric, aggregator, cohort,
                      **kwargs):
    """
        Builds a timeseries dataset for a given metric.

            Parameters:
                - **start**: str or datetime. date + time indicating start of
                    time series
                - **end**: str or datetime. date + time indicating end of
                    time series
                - **interval**: int. integer value in hours that defines the
                    amount of time between data-points
                - **metric**: class object. Metrics class (derived from
                    UserMetric)
                - **aggregator**: method. Aggregator method used to
                    aggregate data for time series data points
                - **cohort**: list(str). list of user IDs
        e.g.

        >>> cohort = ['156171','13234584']
        >>> metric = ba.BytesAdded
        >>> aggregator = agg.list_sum_indices

        >>> build_time_series('20120101000000', '20120112000000', 24, metric,
                aggregator, cohort,
            num_threads=4, num_threads_metric=2, log=True)

    """

    log = bool(kwargs['log']) if 'log' in kwargs else False

    # Get datetime types, and the number of threads
    start = date_parse(um.UserMetric._get_timestamp(start))
    end = date_parse(um.UserMetric._get_timestamp(end))
    k = kwargs['num_threads'] if 'num_threads' in kwargs else 1

    # Compute window size and ensure that all the conditions
    # necessary to generate a proper time series are met
    num_intervals = int((end - start).total_seconds() / (3600 * interval))
    intervals_per_thread = num_intervals / k

    # Compose the sets of time series lists
    f = lambda t,i:  t + datetime.timedelta(
        hours = intervals_per_thread * interval * i)
    time_series = [_get_timeseries(f(start, i),
        f(start, i+1), interval) for i in xrange(k)]
    if f(start, k) <  end: time_series.append(
        _get_timeseries(f(start, k), end, interval))

    data = list()
    q = Queue()
    processes = list()

    if log: logging.info(
        'Spawning procs, %s - %s, interval = %s, threads = %s ... ' % (
        str(start), str(end), interval, k))
    for i in xrange(len(time_series)):
        p = Process(
            target=time_series_worker, args=(
                time_series[i], metric, aggregator, cohort, kwargs, q))
        p.start()
        processes.append(p)

    while 1:
        # sleep before checking worker threads
        time.sleep(PROCESS_SLEEP_TIME)

        if log:
            logging.info('Process queue, %s threads.' % str(len(processes)))

        while not q.empty():
            data.extend(q.get())
        for p in processes:
            if not p.is_alive():
                p.terminate()
                processes.remove(p)

        # exit if all process have finished
        if not len(processes):
            break

    # sort
    return sorted(data, key=operator.itemgetter(0), reverse=False)

def time_series_worker(time_series, metric, aggregator, cohort, kwargs, q):
    """ worker thread which computes time series data for a set of points """
    log = bool(kwargs['log']) if 'log' in kwargs else False

    data = list()
    ts_s = time_series.next()
    new_kwargs = deepcopy(kwargs)

    # re-map some keyword args relating to thread counts
    if 'metric_threads' in new_kwargs:
        d = json.loads(new_kwargs['metric_threads'])
        for key in d: new_kwargs[key] = d[key]
        del new_kwargs['metric_threads']

    while 1:
        try: ts_e = time_series.next()
        except StopIteration: break

        if log: logging.info(__name__ +
                             ' :: Processing thread %s, %s - %s ...' % (
            os.getpid(), str(ts_s), str(ts_e)))

        metric_obj = metric(date_start=ts_s,date_end=ts_e,**new_kwargs).\
            process(cohort, **new_kwargs)

        r = um.aggregator(aggregator, metric_obj, metric.header())

        if log: logging.info(__name__ +
                             ' :: Processing complete %s, %s - %s ...' % (
                                 os.getpid(), str(ts_s), str(ts_e)))

        data.append([str(ts_s), str(ts_e)] + r.data)
        ts_s = ts_e
    q.put(data) # add the data to the queue

class TimeSeriesException(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Could not generate time series."):
        Exception.__init__(self, message)

if __name__ == '__main__':

    cohort = ['156171','13234584']
    metric = rr.RevertRate
    aggregator = rr.reverted_revs_agg

    print build_time_series('20120101000000', '20120201000000', 24, metric,
        aggregator, cohort,
        num_threads=4,
        metric_threads='{"num_threads" : 20, "rev_threads" : 50}', log=True)