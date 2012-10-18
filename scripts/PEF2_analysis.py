#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
	PEF2_analysis.py - Script used to produce filtered PEF-1 users and to generate volume metrics tables

    Dario created db42:dartar.e3_pef_iter2_users - bucketed users on PEF-2

    Filters applied:

        #. Made at least one edit in some namespace
        #. Was not blocked
        #. Global registration date and local registration date are no more than 7 days apart

	Usage:

	Example:
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import settings
sys.path.append(settings.__project_home__)

import logging
import argparse
import datetime
import src.etl.data_loader as dl
import src.metrics.bytes_added as ba
import src.metrics.time_to_threshold as ttt
import src.metrics.blocks as b
from dateutil.parser import *

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    # initialize dataloader objects
    global data_loader
    data_loader = dl.Handle(db='slave')

    # Load eligible users
    sql = 'select user_id from staging.e3_pef_iter2_bucketed_users'
    eligible_users = data_loader.cast_elems_to_string(data_loader.get_elem_from_nested_list(data_loader.execute_SQL(sql),0))
    user_id_str = data_loader.format_comma_separated_list(eligible_users)

    # Create a table storing edits by namespace
    if args.ns_table:
        data_loader.execute_SQL('drop table if exists e3_pef_iter2_ns')

        sql = """
                create table e3_pef_iter2_ns
                    select
                        r.rev_user,
                        p.page_namespace,
                        count(*) as revisions
                    from enwiki.revision as r
                    join enwiki.page as p
                    on r.rev_page = p.page_id
                    where rev_user in (%(users)s)
                    group by 1,2
                    order by 1 asc, 2 asc
            """ % { 'users' : user_id_str }

        data_loader.execute_SQL(" ".join(sql.strip().split()))

    # Get eligible users
    eligible_users = data_loader.cast_elems_to_string(data_loader.get_elem_from_nested_list(data_loader.execute_SQL('select distinct rev_user from e3_pef_iter2_ns'),0))
    logging.info('There are %s eligible user.  Processing ...' % len(eligible_users))
    experiment_start_date = datetime.datetime(year=2012,month=9,day=20)

    # PROCESS TIME TO THRESHOLD
    if args.bytes_added_table:
        get_bytes_added(eligible_users, experiment_start_date)

    # PROCESS TIME TO THRESHOLD
    if args.time_to_threshold_table:
        get_time_to_threshold(eligible_users)

    # PROCESS BLOCKS
    if args.blocks_table:
        get_blocks(experiment_start_date)

    if args.gloabal_user_diff_table:
        sql = read_file(settings.__sql_home__ + 'create_e3_global_user_diff.sql')
        data_loader.execute_SQL(sql)

def read_file(filepath_name):

    sql_file_obj = open(filepath_name,'r')

    sql = ''
    line = sql_file_obj.readline()
    while line:
        sql += line + ' '
        line = sql_file_obj.readline()
    sql_file_obj.close()

    return sql

def get_blocks(experiment_start_date):
    """ Process blocks """

    sql = """
            select
                r.user_id,
                user.user_name
            from (  select
                        distinct rev_user as user_id
                    from e3_pef_iter2_ns) as r
            join enwiki.user on r.user_id = user.user_id
          """
    results = data_loader.execute_SQL(sql)

    # dump results to hash
    h = dict()
    for row in results:
        user_handle = row[1]
        try:
            user_handle = user_handle.encode('utf-8').replace(" ", "_")
        except UnicodeDecodeError:
            user_handle = user_handle.replace(" ", "_")
        h[user_handle] = row[0]

    logging.info("Processing blocked users for %s" % str(experiment_start_date))
    eligible_user_names = data_loader.cast_elems_to_string(data_loader.get_elem_from_nested_list(results,1))
    block_list = b.Blocks(date_start=str(experiment_start_date)).process(eligible_user_names)._results

    # Replace user names with IDs
    for i in xrange(len(block_list)):
        try:
            block_list[i][0] = str(h[block_list[i][0]])
        except KeyError:
            logging.error('Cannot include %s in result.' % block_list[i][0])
            pass

    data_loader.list_to_xsv(block_list)
    sql = read_file(settings.__sql_home__ + 'create_e3_pef_iter2_blocks.sql')
    data_loader.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_blocks')

def get_bytes_added(eligible_users, experiment_start_date):
    """ process "bytes added" """

    sql_reg_date = 'select user_registration from enwiki.user where user_id = %s;'

    bytes_added = list()
    bad_users = 0
    count = 0
    for user in eligible_users:

        if count % 500 == 0:
            logging.info('Processed %s eligible users...' % count)

        try:

            reg_date = parse(data_loader.execute_SQL(sql_reg_date % user)[0][0])
            end_date = reg_date + datetime.timedelta(days=14)

            entry = ba.BytesAdded(date_start=reg_date, date_end=end_date).process([user]).__iter__().next()
            entry.append(str((reg_date-experiment_start_date).seconds / 3600))
            bytes_added.append(entry)

        except Exception as e:
            logging.error('Could not get bytes added for user %s: %s' % (str(user), str(e)))
            bad_users += 1

        count += 1

    logging.info('Missed %s users out of %s.' % (str(bad_users), str(len(eligible_users))))
    logging.info('Writing results to table.')

    data_loader.list_to_xsv(bytes_added)

    # Create table
    data_loader.execute_SQL('drop table if exists e3_pef_iter2_bytesadded')
    sql = read_file(settings.__sql_home__ + 'create_e3_pef_iter2_bytes_added.sql')
    data_loader.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_bytesadded')

    sql = """
                select
                    r.user_id,
                    d.bucket,
                    r.hour_offset,
                    r.bytes_added_net,
                    r.bytes_added_abs,
                    r.bytes_added_pos,
                    r.bytes_added_net,
                    r.edit_count
                from e3_pef_iter2_bytesadded as r
                    join e3_pef_iter2_bucketed_users as d
                    on d.user_id = r.user_id
            """

    data_loader.create_xsv_from_SQL(" ".join(sql.strip().split()), outfile = 'e3_pef_iter2_ba_bucket.tsv')

def get_time_to_threshold(eligible_users):
    """
        Time to Threshold metric processing
    """

    # create table
    sql = read_file(settings.__sql_home__ + 'create_e3_pef_iter2_timetothreshold.sql')

    t = ttt.TimeToThreshold(ttt.TimeToThreshold.EditCountThreshold,
        first_edit=1, threshold_edit=2).process(eligible_users)
    data_loader.list_to_xsv(t)
    data_loader.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_timetothreshold')

    sql = """
            select
                r.user_id,
                d.bucket,
                r.time_minutes
            from e3_pef_iter2_timetothreshold as r
                join e3_pef_iter2_bucketed_users as d on d.user_id = r.user_id
        """
    data_loader.create_xsv_from_SQL(" ".join(sql.strip().split()),outfile = 'e3_pef_iter1_ttt_bucket.tsv')

    return t

# Call Main
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="",
        epilog="python run.py",
        conflict_handler="resolve"
    )

    parser.add_argument('-n', '--ns_table',type=bool, help='',default=False)
    parser.add_argument('-b', '--blocks_table',type=bool, help='',default=False)
    parser.add_argument('-y', '--bytes_added_table',type=bool, help='',default=False)
    parser.add_argument('-t', '--time_to_threshold_table',type=bool, help='',default=False)
    parser.add_argument('-g', '--gloabal_user_diff_table',type=bool, help='',default=True)

    args = parser.parse_args()
    logging.info('Arguments: %s' % args)

    sys.exit(main(args))
