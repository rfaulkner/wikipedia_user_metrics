#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Generate
"""

__author__ = "ryan faulkner"
__date__ = "12/07/2012"
__license__ = "GPL (version 2 or later)"

import sys
import datetime
import logging
import argparse
# import multiprocessing
from dateutil.parser import parse as date_parse

import src.etl.data_loader as dl
import src.metrics.threshold as th

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    # build intervals
    try:
        interval = [1,24,168,720][args.resolution]
    except IndexError:
        logging.error('Specify valid resolution int. 0=hour,1=day,2=week,3=month.')
        return

    c = date_parse(args.date_start)
    e = date_parse(args.date_end)
    ts_list = list()

    while c < e:
        ts_list.append(c)
        c += datetime.timedelta(hours=interval)

    # Temporary: implement for threshold
    # @TODO - abstract out of script
    data=list()
    for i in xrange(len(ts_list) - 1):
        total=0
        pos=0
        for r in th.Threshold(date_start=ts_list[i], date_end=ts_list[i+1],n=1,t=1440).process([]).__iter__():
            try:
                if r[1]: pos+=1
            except IndexError: continue
            except TypeError: continue
            total+=1
        data.append((ts_list[i], float(pos) / total))

    print data
    # Write to a tsv
    dl.DataLoader().list_to_xsv(data)

if __name__ == "__main__":

    # Initialize query date constraints
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)

    today = "".join([today.strftime('%Y%m%d'), "000000"])
    yesterday = "".join([yesterday.strftime('%Y%m%d'),"000000"])
    out_filename = 'dau_out_' + yesterday[:8] + '-' + today[:8] + '.tsv'

    parser = argparse.ArgumentParser(
        description="This script computes generates a tsv conataning rev counts for dailiy active users.",
        epilog="",
        conflict_handler="resolve",
        usage = "./daily_active_users [-n NUMTHREADS] [-m MINEDIT] [-l LOGFREQUENCY] [-s DATE_START] "\
                "[-e DATE_END] [-o OUTFILE]"
    )

    parser.add_argument('-r', '--resolution',type=int, help='0=hour,1=day,2=week,3=month.',default=1)
    parser.add_argument('-s', '--date_start',type=str, help='Start date of measurement.', default=yesterday)
    parser.add_argument('-e', '--date_end',type=str, help='End date of measurement.', default=today)
    args = parser.parse_args()

    sys.exit(main(args))




