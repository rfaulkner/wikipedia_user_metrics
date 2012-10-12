#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
	ACUX_analysis.py - Script used to produce filtered ACUX users and to generate metrics

    Filters applied:

        #. Made at least one edit in some namespace
        #. Was not blocked
        #. Global registration date and local registration date are no more than 7 days apart

	Usage:

        ./ACUX_analysis.py [OPTS]

	Example:

	    ./ACUX_analysis.py -l True -c True      # Loads the click tracking logs + determines ACUX event clickthrough
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "October 11th, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import settings
sys.path.append(settings.__project_home__)

import logging
import argparse
import libraries.etl.DataLoader as DL
import libraries.etl.ExperimentsLoader as EL

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    logging.info(str(args))

    # initialize dataloader objects
    global dl
    dl = DL.DataLoader(db='slave')

    global el
    el = EL.ExperimentsLoader()

    if args.load:
        load_log_data()

    if args.clickthrough:
        get_click_through()


def read_file(filepath_name):

    sql_file_obj = open(filepath_name,'r')

    sql = ''
    line = sql_file_obj.readline()
    while (line):
        sql += line + ' '
        line = sql_file_obj.readline()
    sql_file_obj.close()

    return sql

def load_log_data():
    # Create tables to store log data
    sql = read_file(settings.__sql_home__ + 'create_e3_acux_client_events.sql')
    dl.execute_SQL('drop table if exists e3_acux1_client_events')
    dl.execute_SQL(sql)

    sql = read_file(settings.__sql_home__ + 'create_e3_acux_server_events.sql')
    dl.execute_SQL('drop table if exists e3_acux1_server_events')
    dl.execute_SQL(sql)

    lpm = DL.DataLoader.LineParseMethods()

    # Load users from logs

    logs = ['clicktracking.log-20121004.gz','clicktracking.log-20121005.gz','clicktracking.log-20121006.gz',
            'clicktracking.log-20121007.gz','clicktracking.log-20121008.gz','clicktracking.log-20121009.gz',
            'clicktracking.log-20121010.gz',]

    for log in logs:
        dl.create_table_from_xsv(log,'','e3_acux1_client_events',
            parse_function=lpm.e3_acux_log_parse, regex_list=['ext.accountCreationUX'], header=False)
        dl.create_table_from_xsv(log,'','e3_acux1_server_events',
            parse_function=lpm.e3_acux_log_parse, regex_list=['event_id=account_create'], header=False)


def get_click_through():
    return

def get_blocks(experiment_start_date):
    return

def get_bytes_added(eligible_users, experiment_start_date):
    return

def get_time_to_threshold(eligible_users):
    return

# Call Main
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="This script performs ETL for Account Creation experimental data.",
        epilog="",
        conflict_handler="resolve"
    )
    parser.add_argument('-l', '--load',type=str, help='Load the clicktracking logs.',default=False)
    parser.add_argument('-c', '--clickthrough',type=str, help='Compute event click through.',default=False)

    args = parser.parse_args()

    sys.exit(main(args))
