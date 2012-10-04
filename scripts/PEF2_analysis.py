#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
	PEF-1_analysis.py - Script used to produce filtered PEF-1 users and to generate volume metrics tables

    Dario created db42:dartar.e3_pef_iter2_users - bucketed users on PEF-2

    Filters appliexd:

        #. Made at least one edit in some namespace
        #. Was not blocked
        #. Global registration date and local registration date are no more than 7 days apart

	Usage:

	Example:
"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__license__ = "GPL (version 2 or later)"

import sys
import settings
sys.path.append(settings.__project_home__)

import logging
import datetime
import libraries.etl.DataLoader as DL
import libraries.etl.ExperimentsLoader as EL
import libraries.metrics.BytesAdded as BA
import libraries.metrics.TimeToThreshold as TTT
import libraries.metrics.Blocks as B
from dateutil.parser import *

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):


    # initialize dataloader objects
    global dl
    dl = DL.DataLoader(db='slave')

    global el
    el = EL.ExperimentsLoader()

    # Load eligible users
    #sql = 'select user_id from dartar.e3_pef_iter2_users'
    #eligible_users = dl.cast_elems_to_string(dl.get_elem_from_nested_list(dl.execute_SQL(sql),0))


    # Verify that the user made at least one edit
    # dl.execute_SQL('create table e3_pef_iter2_ns select r.rev_user, p.page_namespace, count(*) as revisions from enwiki.revision as r join enwiki.page as p on r.rev_page = p.page_id where rev_user in (%s) group by 1,2 order by 1 asc, 2 asc' % user_id_str)
    eligible_users = dl.cast_elems_to_string(dl.get_elem_from_nested_list(dl.execute_SQL('select distinct rev_user from rfaulk.e3_pef_iter2_ns'),0))

    # user_id_str = dl.format_comma_separated_list(eligible_users, include_quotes=False)

    logging.info('There are %s eligible user.  Processing ...' % len(eligible_users))
    experiment_start_date = datetime.datetime(year=2012,month=9,day=20)

    # PROCESS TIME TO THRESHOLD
    # ========================
    # get_bytes_added(experiment_start_date)

    # PROCESS TIME TO THRESHOLD
    # ========================
    # get_time_to_threshold(eligible_users)

    # PROCESS BLOCKS
    # ==============
    get_blocks(experiment_start_date)

def read_file(filepath_name):

    sql_file_obj = open(filepath_name,'r')

    sql = ''
    line = sql_file_obj.readline()
    while (line):
        sql += line + ' '
        line = sql_file_obj.readline()
    sql_file_obj.close()

    return sql

def get_blocks(experiment_start_date):
    """

    """

    sql = 'select r.user_id, user.user_name from (select distinct rev_user as user_id from rfaulk.e3_pef_iter2_ns) as r join enwiki.user on r.user_id = user.user_id'
    results = dl.execute_SQL(sql)

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
    eligible_user_names = dl.cast_elems_to_string(dl.get_elem_from_nested_list(results,1))
    block_list = B.Blocks(date_start=str(experiment_start_date)).process(eligible_user_names)

    # Replace user names with IDs
    for i in range(len(block_list)):
        try:
            block_list[i][0] = str(h[block_list[i][0]])
        except KeyError:
            logging.error('Cannot include %s in result.' % block_list[i][0])
            pass

    dl.list_to_xsv(block_list)
    sql = read_file(settings.__sql_home__ + 'create_e3_pef_iter2_blocks.sql')
    dl.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_blocks', create_table=True)

def get_bytes_added(eligible_users, experiment_start_date):
    """
        PROCESS "BYTES ADDED"
    """

    sql_reg_date = 'select user_registration from enwiki.user where user_id = %s;'

    bytes_added = list()
    bad_users = 0
    count = 0
    for user in eligible_users:

        if count % 500 == 0:
            logging.info('Processed %s eligible users...' % count)

        try:

            reg_date = parse(dl.execute_SQL(sql_reg_date % user)[0][0])
            end_date = reg_date + datetime.timedelta(days=14)

            # logging.debug('Reg date = %s' % str(reg_date))

            r = BA.BytesAdded(date_start=reg_date, date_end=end_date, raw_count=False).process([user])
            key = r.keys()[0]

            entry = list()
            entry.append(key)
            entry.append(str((reg_date-experiment_start_date).seconds / 3600))
            entry.extend(r[key])

            bytes_added.append(entry)

        except Exception as e:
            logging.error('Could not get for user %s, bytes added: %s' % (str(user), str(e)))
            bad_users += 1

        count += 1

    logging.info('Missed %s users out of %s.' % (str(bad_users), str(len(eligible_users))))
    logging.info('Writing results to table.')

    dl.list_to_xsv(bytes_added)

    # Create table
    sql = read_file(settings.__sql_home__ + 'create_e3_pef_iter2_bytes_added.sql')
    dl.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_bytesadded', create_table=True)
    dl.create_xsv_from_SQL("select r.user_id, d.bucket, r.hour_offset, r.bytes_added_net, r.bytes_added_abs, r.bytes_added_pos, r.bytes_added_neg, r.edit_count " +\
                       "from rfaulk.e3_pef_iter2_bytesadded as r join dartar.e3_pef_iter2_users as d on d.user_id = r.user_id;",
    outfile = 'e3_pef_iter2_ba_bucket.tsv')

    return bytes_added

def get_time_to_threshold(eligible_users):
    """
        Time to Threshold metric processing
    """

    # create table
    sql = read_file(settings.__sql_home__ + 'create_e3_pef_iter2_timetothreshold.sql')

    ttt = TTT.TimeToThreshold(TTT.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2).process(eligible_users)
    dl.list_to_xsv(ttt)
    dl.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_timetothreshold', create_table=True)
    dl.create_xsv_from_SQL('select r.user_id, d.bucket, r.time_minutes from rfaulk.e3_pef_iter2_timetothreshold as r join dartar.e3_pef_iter2_users as d on d.user_id = r.user_id;', outfile = 'e3_pef_iter1_ttt_bucket.tsv')

    return ttt

# Call Main
if __name__ == "__main__":
    sys.exit(main([]))
