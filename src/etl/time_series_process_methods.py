
"""
    This class stores custom methods to extract time series data.
"""

__author__ = "ryan faulkner"
__date__ = "12/10/2012"
__license__ = "GPL (version 2 or later)"

import datetime
from dateutil.parser import parse as date_parse
import src.metrics.threshold as th

def productive_editors_by_day(args, interval,log=True):
    """ Computes a list of threshold metrics """

    c = date_parse(args.date_start)
    e = date_parse(args.date_end)
    ts_list = list()

    while c < e:
        ts_list.append(c)
        c += datetime.timedelta(hours=interval)

    for i in xrange(len(ts_list) - 1):
        total=0
        pos=0

        if log:
            print str(datetime.datetime.now()) + ' - Processing time series data for %s...' % str(ts_list[i])

        for r in th.Threshold(date_start=ts_list[i], date_end=ts_list[i+1],n=1,t=1440).process([]).__iter__():
            try:
                if r[1]: pos+=1
            except IndexError: continue
            except TypeError: continue
            total+=1

        if log:
            print " ".join(['timestamp = ', str(ts_list[i]), ', total registrations = ',
                            str(total), ', % prod editors = ',str(float(pos) / total)])

        yield (ts_list[i], total, float(pos) / total)
        # yields: (timestamp, total registrations, fraction of productive)

class DataTypeMethods(object):
    DATA_TYPE = {'prod' : productive_editors_by_day}



