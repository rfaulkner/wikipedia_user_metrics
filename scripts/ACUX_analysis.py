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
import src.etl.data_loader as dl
import src.etl.experiments_loader as el

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    logging.info(str(args))

    # initialize dataloader objects
    global data_loader
    data_loader = dl.DataLoader(db='slave')

    global exp_loader
    exp_loader = el.ExperimentsLoader()

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

    global data_loader

    # Create tables to store log data
    sql = read_file(settings.__sql_home__ + 'create_e3_acux_client_events.sql')
    data_loader.execute_SQL('drop table if exists e3_acux1_client_events')
    data_loader.execute_SQL(sql)

    sql = read_file(settings.__sql_home__ + 'create_e3_acux_server_events.sql')
    data_loader.execute_SQL('drop table if exists e3_acux1_server_events')
    data_loader.execute_SQL(sql)

    lpm = dl.DataLoader.LineParseMethods()

    # Load users from logs

    logs = ['clicktracking.log-20121004.gz','clicktracking.log-20121005.gz','clicktracking.log-20121006.gz',
            'clicktracking.log-20121007.gz','clicktracking.log-20121008.gz','clicktracking.log-20121009.gz',
            'clicktracking.log-20121010.gz',]

    for log in logs:
        data_loader.create_table_from_xsv(log,'','e3_acux1_client_events',
            parse_function=lpm.e3_acux_log_parse, regex_list=['ext.accountCreationUX'], header=False)
        data_loader.create_table_from_xsv(log,'','e3_acux1_server_events',
            parse_function=lpm.e3_acux_log_parse, regex_list=['event_id=account_create'], header=False)


def get_click_through():

    global data_loader

    logging.info('Create table of de-duped events.')
    sql = """
                CREATE TABLE e3_acux1_deduped AS
                (SELECT
                    e3_acux_hash AS user_token,
                    if(e3_acux_event regexp "control", "control", "acux_1") AS bucket,
                    if(e3_acux_event regexp "assignment", "a",
                    if(e3_acux_event regexp "impression", "i",
                    if(e3_acux_event regexp "submit", "s", "unknown"))) AS event_type,
                    min(e3_acux_timestamp) as first_timestamp,
                    max(e3_acux_timestamp) as last_timestamp
                FROM e3_acux1_client_events
                WHERE (e3_acux_event regexp "assignment" OR
                    e3_acux_event regexp "impression" OR
                    e3_acux_event regexp "submit") AND
                    e3_acux_hash != ''
                GROUP BY 1,2,3)

                UNION

                (SELECT
                    mw_user_token as user_token,
                    user_id as bucket,
                    "c" as event_type,
                    min(timestamp) as first_timestamp,
                    max(timestamp) as last_timestamp
                FROM e3_acux1_server_events
                WHERE self_made = 1
                GROUP BY 1,2,3)
            """
    sql = " ".join(sql.strip().split())
    data_loader.execute_SQL('drop table if exists e3_acux1_deduped')
    data_loader._cur_.execute(" ".join(sql.strip().split()))

    # get counts for client side events
    sql = """
            SELECT bucket, event_type, count(*) AS instances
            FROM e3_acux1_deduped
            WHERE event_type = "a" OR event_type = "i" OR event_type = "s" GROUP BY 1,2;
        """
    sql = " ".join(sql.strip().split())
    # client_event_click_results = dl._cur_.execute(" ".join(sql.strip().split()))

    # Get the buckets for "create" events
    logging.info('Create table for create events.')
    sql = """
            CREATE TABLE e3_acux1_created AS
            (SELECT
                e1.user_token,
                e1.bucket as user_id,
                e2.bucket,
                e1.first_timestamp,
                e1.last_timestamp
            FROM
                (SELECT *
                 FROM e3_acux1_deduped
                 WHERE event_type = "c") AS e1
            JOIN (SELECT user_token, bucket
                 FROM e3_acux1_deduped
                 WHERE event_type != "c"
                 GROUP BY 1,2) AS e2
            ON e1.user_token = e2.user_token)
        """
    sql = " ".join(sql.strip().split())
    data_loader.execute_SQL('drop table if exists e3_acux1_created')
    data_loader._cur_.execute(" ".join(sql.strip().split()))

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
    parser.add_argument('-l', '--load',type=bool, help='Load the clicktracking logs.',default=False)
    parser.add_argument('-c', '--clickthrough',type=bool, help='Compute event click through.',default=False)

    args = parser.parse_args()

    sys.exit(main(args))
