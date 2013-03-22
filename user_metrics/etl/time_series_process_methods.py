
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

from user_metrics.config import settings
import user_metrics.metrics.user_metric as um
from user_metrics.utils import format_mediawiki_timestamp
from multiprocessing import Process, Queue

from user_metrics.config import logging

# Determines the amount of time to wait before picking completed threads off
# of the queue
MAX_THREADS = settings.__time_series_thread_max__
PROCESS_SLEEP_TIME = 4


def _get_timeseries(date_start, date_end, interval):
    """
        Generates a series of timestamps given a start date,
        end date, and interval
    """

    # Ensure the dates are string representations
    date_start = format_mediawiki_timestamp(date_start)
    date_end = format_mediawiki_timestamp(date_end)

    c = date_parse(date_start) + datetime.timedelta(hours=-int(interval))
    e = date_parse(date_end)
    while c < e:
        c += datetime.timedelta(hours=int(interval))
        yield c


def build_time_series(start, end, interval, metric, aggregator, cohort,
                      **kwargs):
    """
        Builds a timeseries dataset for a given metric.

        Parameters:

            start: str or datetime.
                date + time indicating start of time series

            end : str or datetime.
                date + time indicating end of time series

            interval : int.
                integer value in hours that defines the amount of
                time between data-points

            metric : class object.
                Metrics class (derived from UserMetric)

            aggregator : method.
                Aggregator method used to aggregate data for time
                series data points

            cohort : list(str).
                list of user IDs

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
    start = date_parse(format_mediawiki_timestamp(start))
    end = date_parse(format_mediawiki_timestamp(end))
    k = kwargs['kt_'] if 'kt_' in kwargs else MAX_THREADS

    # Compute window size and ensure that all the conditions
    # necessary to generate a proper time series are met
    num_intervals = int((end - start).total_seconds() / (3600 * interval))
    intervals_per_thread = num_intervals / k

    # Compose the sets of time series lists
    f = lambda t, i:  t + datetime.timedelta(
        hours=int(intervals_per_thread * interval * i))
    time_series = [_get_timeseries(f(start, i),
                   f(start, i+1), interval) for i in xrange(k)]
    if f(start, k) < end:
        time_series.append(_get_timeseries(f(start, k), end, interval))

    event_queue = Queue()
    process_queue = list()

    if log:
        logging.info(__name__ + ' :: Spawning procs\n'
                                '\t%s - %s, interval = %s\n'
                                '\tthreads = %s ... ' % (str(start), str(end),
                                                       interval, k))
    for i in xrange(len(time_series)):
        p = Process(target=time_series_worker,
                    args=(time_series[i], metric, aggregator,
                          cohort, event_queue, kwargs))
        p.start()
        process_queue.append(p)

    # Call the listener
    return time_series_listener(process_queue, event_queue)


def time_series_listener(process_queue, event_queue):
    """
        Listener for ``time_series_worker``.  Blocks and logs until all
        processes computing time series data are complete.  Returns time
        dependent data from metrics.

        Parameters
        ~~~~~~~~~~

            process_queue : list
                List of active processes computing metrics data.

            event_queue : multiprocessing.Queue
                Asynchronous data coming in from worker processes.
    """
    data = list()

    while 1:
        # sleep before checking worker threads
        time.sleep(PROCESS_SLEEP_TIME)

        logging.info(__name__ + ' :: Time series process queue\n'
                                '\t{0} threads. (PID = {1})'.
            format(str(len(process_queue)), os.getpid()))

        while not event_queue.empty():
            data.extend(event_queue.get())
        for p in process_queue:
            if not p.is_alive():
                p.terminate()
                process_queue.remove(p)

        # exit if all process have finished
        if not len(process_queue):
            break

    # sort
    return sorted(data, key=operator.itemgetter(0), reverse=False)


def time_series_worker(time_series,
                       metric,
                       aggregator,
                       cohort,
                       event_queue,
                       kwargs):
    """
        Worker thread which computes time series data for a set of points

        Parameter
        ~~~~~~~~~

            time_series : list(datetime)
                Datetimes defining series.

            metric : string
                Metric name.

            aggregator : method
                aggregator method reference.

            cohort : string
                Cohort name.

            event_queue : multiporcessing.Queue
                Asynchronous data-structure to communicate with parent proc.
    """
    log = bool(kwargs['log']) if 'log' in kwargs else False

    data = list()
    ts_s = time_series.next()
    new_kwargs = deepcopy(kwargs)

    # re-map some keyword args relating to thread counts
    if 'metric_threads' in new_kwargs:
        d = json.loads(new_kwargs['metric_threads'])
        for key in d:
            new_kwargs[key] = d[key]
        del new_kwargs['metric_threads']

    while 1:
        try:
            ts_e = time_series.next()
        except StopIteration:
            break

        if log:
            logging.info(__name__ + ' :: Processing thread:\n'
                                    '\t{0}, {1} - {2} ...'.format(os.getpid(),
                                                                  str(ts_s),
                                                                  str(ts_e)))

        metric_obj = metric(datetime_start=ts_s, datetime_end=ts_e, **new_kwargs).\
            process(cohort, **new_kwargs)

        r = um.aggregator(aggregator, metric_obj, metric.header())

        if log:
            logging.info(__name__ + ' :: Processing complete:\n'
                                    '\t{0}, {1} - {2} ...'.format(os.getpid(),
                                                                  str(ts_s),
                                                                  str(ts_e)))
        data.append([str(ts_s), str(ts_e)] + r.data)
        ts_s = ts_e

    event_queue.put(data)


class TimeSeriesException(Exception):
    """ Basic exception class for UserMetric types """
    def __init__(self, message="Could not generate time series."):
        Exception.__init__(self, message)
