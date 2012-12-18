
"""
    This module contains custom methods to extract time series data.
"""

__author__ = "ryan faulkner"
__date__ = "12/10/2012"
__license__ = "GPL (version 2 or later)"

import datetime
from dateutil.parser import parse as date_parse
import src.metrics.threshold as th
import src.metrics.revert_rate as rr
import src.etl.aggregator as agg
import src.etl.data_loader as dl

def _get_timeseries(date_start, date_end, interval):
    """ Generates a series of timestamps given a start date, end date, and interval"""
    c = date_parse(date_start) + datetime.timedelta(hours=-interval)
    e = date_parse(date_end)
    while c < e:
        c += datetime.timedelta(hours=interval)
        yield c

def _get_newly_registered_users(date_start, date_end, project):
    """ """
    sql = """
            select
                log_user
            from %(project)s.logging
            where log_timestamp >= %(date_start)s and log_timestamp <= %(date_end)s and
                log_action = 'create' AND log_type='newusers'
        """ % {
        'project' : project,
        'date_start' : date_start,
        'date_end' : date_end
    }
    return dl.DataLoader().get_elem_from_nested_list(
        dl.Connector(instance='slave').execute_SQL(" ".join(sql.strip().split('\n'))),0)

def threshold_editors(args,interval,log=True,project='enwiki',k=1,n=1,t=1440):
    """ Computes a list of threshold metrics """

    time_series = _get_timeseries(args.date_start, args.date_end, interval)

    ts_s = time_series.next()
    while 1:
        try:
            ts_e = time_series.next()
        except StopIteration:
            break

        if log: print str(datetime.datetime.now()) + ' - Processing time series data for %s...' % str(ts_s)
        user_ids = _get_newly_registered_users(args.date_start, args.date_end,project)

        # Build an iterator across users for a given threshold
        threshold_obj = th.Threshold(date_start=ts_s, date_end=ts_e,n=n,t=t).process(user_ids,
            log_progress=True, num_threads=k)
        total, pos, rate = agg.threshold_editors_agg(threshold_obj)

        if log: print " ".join(['timestamp = ', str(ts_s), ', total registrations = ',
                            str(total), ', % prod editors = ',str(rate)])

        yield (ts_s, total, rate) # yields: (timestamp, total registrations, fraction of productive)
        ts_s = ts_e


def reverted(args,interval,log=True,project='enwiki',la=15,lb=15,k=1):

    """ Computes a list of threshold metrics """

    time_series = _get_timeseries(args.date_start, args.date_end, interval)
    ts_s = time_series.next()

    while 1:
        try:
            ts_e = time_series.next()
        except StopIteration:
            break

        if log: print str(datetime.datetime.now()) + ' - Processing time series data for %s...' % str(ts_s)
        user_ids = _get_newly_registered_users(args.date_start, args.date_end,project)

        revert_obj = rr.RevertRate(date_start=ts_s, date_end=ts_e,look_ahead=la,look_back=lb).process(user_ids,
            log_progress=True, num_threads=k)
        total_revs, weighted_rate, total_editors, reverted_editors = agg.reverted_revs_agg(revert_obj)

        if log:
            print " ".join(['timestamp = ', str(ts_s), ', total revisions = ',
                            str(total_revs), ', total revert rate = ',str(weighted_rate),
                            ' total editors = ', str(total_editors), ' reverted editors = ', str(reverted_editors)])

        yield (ts_s, total_revs, weighted_rate, total_editors, reverted_editors)
        ts_s = ts_e

class DataTypeMethods(object):
    DATA_TYPE = {'prod' : threshold_editors, 'revert' : reverted}



