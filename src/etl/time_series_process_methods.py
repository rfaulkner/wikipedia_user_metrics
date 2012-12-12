
"""
    This class stores custom methods to extract time series data.
"""

__author__ = "ryan faulkner"
__date__ = "12/10/2012"
__license__ = "GPL (version 2 or later)"

import datetime
from dateutil.parser import parse as date_parse
import src.metrics.threshold as th

def _get_timeseries(date_start, date_end, interval):
    """ Generates a series of timestamps given a start date, end date, and interval"""
    c = date_parse(date_start) + datetime.timedelta(hours=-interval)
    e = date_parse(date_end)

    while c < e:
        c += datetime.timedelta(hours=interval)
        yield c


def productive_editors(args,interval,log=True,user_ids=None):
    """ Computes a list of threshold metrics """

    if user_ids is None: user_ids = list() # assign an empty list in case default arg is used
    time_series = _get_timeseries(args.date_start, args.date_end, interval)

    ts_s = time_series.next()
    while 1:
        try:
            ts_e = time_series.next()
        except StopIteration:
            break

        total=0
        pos=0

        if log:
            print str(datetime.datetime.now()) + ' - Processing time series data for %s...' % str(ts_s)

        for r in th.Threshold(date_start=ts_s, date_end=ts_e,n=10,t=1440).process(user_ids,
            log_progress=True, num_threads=0).__iter__():
            try:
                if r[1]: pos+=1
            except IndexError: continue
            except TypeError: continue
            total+=1

        if log:
            print " ".join(['timestamp = ', str(ts_s), ', total registrations = ',
                            str(total), ', % prod editors = ',str(float(pos) / total)])

        yield (ts_s, total, float(pos) / total) # yields: (timestamp, total registrations, fraction of productive)
        ts_s = ts_e

class DataTypeMethods(object):
    DATA_TYPE = {'prod' : productive_editors}



