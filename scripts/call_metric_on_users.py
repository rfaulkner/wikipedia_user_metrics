#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
	call_metric_on_users.py - Write metrics to a user list

	Example:

	    RFaulkner-WMF:projects rfaulkner$  ./call_metrics_on_users.py -m "bytes_added" -s "2010-01-01 00:00:00" -e "2012-10-01 00:00:00"
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "October 10th, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import logging
import settings
import datetime
import argparse
from libraries.metrics import BytesAdded as BA
from libraries.metrics import Blocks as B

sys.path.append(settings.__E3_Analysis_Home__)

global metric_types
metric_types = ['bytes_added', 'blocks']

global header
header = []

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    global header

    logging.info('Processing %(metric)s for user list from %(date_start)s to %(date_end)s.'
    % {'metric' : args.metric, 'date_start' : args.date_start, 'date_end' : args.date_end})
    metric = get_metric(args)

    outfile = open(args.output, 'w')
    l = [str(i) for i in header]
    outfile.write(l)

    line = sys.stdin.readline()
    while line:
        user_id = int(line)

        l = [str(i) for i in metric.process(user_id)]
        l.extend('\n')
        outfile.write('\t'.join(l))

        line = sys.stdin.readline()

    logging.info('Transform complete.')

def get_metric(args):
    """ Initializes the metric type based on input """

    global header

    m_index = [metric == args.metric for metric in metric_types].index(True)
    metric_class = [BA.BytesAdded(date_start=args.date_start, date_end=args.date_start, project=args.project, raw_count=True),
                 B.Blocks(date_start=args.date_start, project=args.project, return_list=True, return_generator=False)]
    metric_headers = [BA.BytesAdded.HEADER, B.Blocks.HEADER]
    header = metric_headers[m_index]
    header.extend('\n')

    return metric_class[m_index]

# Call Main
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="This script computes specified user metrics.",
        epilog="",
        conflict_handler="resolve"
    )
    parser.add_argument('-m', '--metric',type=str, help='The metric to compute.',default="bytes_added")
    parser.add_argument('-o', '--output',type=str, help='Output tsv filename.',default="./output.tsv")
    parser.add_argument('-s', '--date_start',type=str, help='Start date of measurement.',default="2008-01-01 00:00:00")
    parser.add_argument('-e', '--date_end',type=str, help='End date of measurement.',default=str(datetime.datetime.now()))
    parser.add_argument('-p', '--project',type=str, help='Project name.',default='enwiki')

    args = parser.parse_args()

    sys.exit(main(args))
