#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Script for generating time series datasets.  Uses module 'src.etl.time_series_process_methods'.
"""

__author__ = "ryan faulkner"
__date__ = "12/07/2012"
__license__ = "GPL (version 2 or later)"

import sys
import e3_settings as s
sys.path.append(s.__E3_Analysis_Home__)

import datetime
import logging
import argparse
import src.etl.data_loader as dl
import src.etl.time_series_process_methods as tspm

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    # build intervals
    try:
        interval = [1,24,168,720][args.resolution]
    except IndexError:
        logging.error('Specify valid resolution int. 0=hour,1=day,2=week,3=month.')
        return

    # if the argument specifying SQL for user IDs has been passed generate uids
    uids=None
    if args.user_sql:
        conn = dl.Connector(instance='slave')
        conn._cur_.execute(args.user_sql)
        uids = [r[0] for r in conn._cur_][:10]

    # Write to a tsv
    try:
        dl.DataLoader().list_to_xsv(tspm.DataTypeMethods.DATA_TYPE[args.data_type](args, interval, user_ids=uids))
    except KeyError:
        logging.info('Invalid data type.')

if __name__ == "__main__":

    # Initialize query date constraints
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)

    today = "".join([today.strftime('%Y%m%d'), "000000"])
    yesterday = "".join([yesterday.strftime('%Y%m%d'),"000000"])

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
    parser.add_argument('-t', '--data_type',type=str, help='Type of data to gather.', default='prod')
    parser.add_argument('-u', '--user_sql',type=str, help='Specify user SQL query to get IDs.', default='')
    args = parser.parse_args()

    sys.exit(main(args))




