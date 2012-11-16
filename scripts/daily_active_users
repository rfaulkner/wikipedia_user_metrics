#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Computes active users over a specified time period
"""
__author__ = "ryan faulkner"
__date__ = "11/06/2012"
__license__ = "GPL (version 2 or later)"

import sys
import settings as s
sys.path.append(s.__E3_Analysis_Home__)

import logging
import datetime
import argparse
import src.metrics.bytes_added as ba

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

EDIT_COUNT_IDX = 5

def main(args):

    logging.info(args)
    logging.info('Processing contribution volume metrics for daily users from %s to %s ... ' %
                 (args.date_start, args.date_end))
    file_obj = open(s.__data_home__ + args.output_file,'w')
    try:
        file_obj.write("\t".join(ba.BytesAdded.header()) + '\n')
        for row in ba.BytesAdded(date_start=args.date_start, date_end=args.date_end).process(
            log_progress=True,log_frequency=args.log_frequency).__iter__():
            try:
                if row[EDIT_COUNT_IDX] >= args.min_edit:
                    file_obj.write("\t".join([str(e) for e in row]) + '\n')
            except IndexError:
                logging.info("Could not write row %s." % str(row))
    finally:
        file_obj.close()

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
        usage = "user_ids | call_metric_on_users.py [-h] [-m METRIC] [-o OUTPUT] [-s DATE_START] [-e DATE_END] [-p PROJECT]"
    )

    parser.add_argument('-m', '--min_edit',type=int, help='minimum amounts of edits.',default=0)
    parser.add_argument('-s', '--date_start',type=str, help='Start date of measurement.',
        default=yesterday)
    parser.add_argument('-e', '--date_end',type=str, help='End date of measurement.', default=today)
    parser.add_argument('-l', '--log_frequency',type=int,help='How often to log progress.',default=0)
    parser.add_argument('-o', '--output_file',type=str,help='File to store output.',default='dau_out.tsv')
    args = parser.parse_args()
    sys.exit(main(args))