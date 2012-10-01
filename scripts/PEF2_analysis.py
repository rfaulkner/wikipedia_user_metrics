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
import classes.DataLoader as DL
import classes.Metrics as M
from dateutil.parser import *

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):


    # initialize dataloader objects
    dl = DL.DataLoader(db='slave')
    el = DL.ExperimentsLoader()

    # Load eligible users
    #sql = 'select user_id from dartar.e3_pef_iter2_users'
    #eligible_users = dl.cast_elems_to_string(dl.get_elem_from_nested_list(dl.execute_SQL(sql),0))


    # Verify that the user made at least one edit
    # dl.execute_SQL('create table e3_pef_iter2_ns select r.rev_user, p.page_namespace, count(*) as revisions from enwiki.revision as r join enwiki.page as p on r.rev_page = p.page_id where rev_user in (%s) group by 1,2 order by 1 asc, 2 asc' % user_id_str)
    eligible_users = dl.cast_elems_to_string(dl.get_elem_from_nested_list(dl.execute_SQL('select distinct user_id from rfaulk.e3_pef_iter1_ns'),0))
    # user_id_str = dl.format_comma_separated_list(eligible_users, include_quotes=False)

    logging.info('There are %s eligible user.  Processing ...' % len(eligible_users))

    sql_reg_date = 'select user_registration from enwiki.user where user_id = %s;'
    experiment_start_date = datetime.datetime(year=2012,month=9,day=20)


    # PROCESS "BYTES ADDED"
    # =====================

    bytes_added = list()
    bad_users = 0
    count = 0
    for user in eligible_users:

        if count % 500 == 0:
            logging.info('Processed %s eligible users...' % count)

        # logging.debug('Processing user %s' % user)

        try:

            reg_date = parse(dl.execute_SQL(sql_reg_date % user)[0][0])
            end_date = reg_date + datetime.timedelta(days=14)

            # logging.debug('Reg date = %s' % str(reg_date))

            r = M.BytesAdded(date_start=reg_date, date_end=end_date, raw_count=False).process([user])
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

    sql_file_obj = open(settings.__sql_home__ + 'create_e3_pef_iter2_bytes_added.sql','r')

    sql = ''
    line = sql_file_obj.readline()
    while (line):
        sql += line + ' '
        line = sql_file_obj.readline()
    sql_file_obj.close()

    dl.execute_SQL(sql)

    dl.create_table_from_xsv('list_to_xsv.out', sql, 'e3_pef_iter2_bytesadded', create_table=True)
    dl.create_xsv_from_SQL('select r.user_id, d.bucket, r.hour_offset, r.bytes_added_net, r.bytes_added_abs, r.bytes_added_pos, r.bytes_added_neg, " + \
    "r.edit_count from rfaulk.e3_pef_iter1_bytesadded as r join dartar.e3_pef_iter2_users as d on d.user_id = r.user_id;',
    outfile = 'e3_pef_iter2_ba_bucket.tsv')

    # PROCESS TIME TO THRESHOLD
    # ========================

    # ttt = M.TimeToThreshold(M.TimeToThreshold.EDIT_COUNT_THRESHOLD, first_edit=1, threshold_edit=2).process(eligible_users)
    # dl.list_to_xsv(ttt)
    # dl.create_table_from_xsv('list_to_xsv.out', '', 'e3_pef_iter2_timetothreshold')
    # dl.create_xsv_from_SQL('select r.user_id, d.bucket, r.time_minutes from rfaulk.e3_pef_iter2_timetothreshold as r join dartar.e3_pef_iter2_users as d on d.user_id = r.user_id;', outfile = 'e3_pef_iter1_ttt_bucket.tsv')


# Call Main
if __name__ == "__main__":
    sys.exit(main([]))
