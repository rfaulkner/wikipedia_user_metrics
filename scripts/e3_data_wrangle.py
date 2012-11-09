#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
	e3_data_wrangle.py - Script used to produce ...
	Usage:

	Example:
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import settings as s
sys.path.append(s.__E3_Analysis_Home__)

import logging
import argparse
import src.etl.data_loader as dl
import e3_experiment_definitions as e3_def

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
    format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

global exp_meta_data
global conn
conn = dl.Connector(instance='slave')

def load_logs():
    global exp_meta_data

    lpm = exp_meta_data['log_parser_method']

    # drop the table if it already exists
    conn._cur_.execute('drop table if exists %s' % exp_meta_data['user_bucket']['table_name'])

    # create the new table
    conn._cur_.execute(" ".join(exp_meta_data['user_bucket']['definition'].strip().split('\n')))

    # load log data
    for f in exp_meta_data['log_files']:
        logging.info('Processing file %s ...' % f)
        dl.DataLoader().create_table_from_xsv(f, '', exp_meta_data['user_bucket']['table_name'],
            parse_function=lpm, header=False)

def main(args):
    global exp_meta_data

    logging.info(args)

    # Load the experiment meta data
    try:
        exp_meta_data = e3_def.experiments[args.experiment]
    except KeyError:
        logging.error('Experiment not found: %s'  % str(args.experiment))
        return

    if args.load_logs:
        load_logs()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="This script filters log data and build metrics from Wikimedia editor engagement experiments.",
        epilog="EXPERIMENT = %s" % str(e3_def.experiments.keys()),
        conflict_handler="resolve",
        usage = "e3_data_wrangle.py [-e EXPERIMENT] [-l]"
    )
    parser.add_argument('-e', '--experiment',type=str, help='Experiment.',default='CTA4')
    parser.add_argument('-l', '--load_logs',action="store_true",help='Process log data.',default=False)

    args = parser.parse_args()
    sys.exit(main(args))
