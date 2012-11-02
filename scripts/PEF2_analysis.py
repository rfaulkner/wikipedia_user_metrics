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
import src.etl.data_filter as df
import src.etl.data_loader as dl
import src.metrics.bytes_added as ba
import src.metrics.time_to_threshold as ttt
import src.metrics.revert_rate as r
import src.metrics.blocks as b
from dateutil.parser import parse as date_parse

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

MYSQL_TABLE_FILTER = 'e3_pef_iter2_filtered_users'
MYSQL_TABLE_BUCKETS = 'e3_pef_iter2_bucketed_users'
MYSQL_TABLE_NAMESPACE = 'e3_pef_iter2_ns2'
MYSQL_TABLE_BLOCKS = 'e3_pef_iter2_blocks2'
MYSQL_TABLE_BYTES_ADDED = 'e3_pef_iter2_bytesadded'
MYSQL_TABLE_REVERTS = 'e3_pef_iter2_revertrate'
MYSQL_TABLE_TTT = 'e3_pef_iter2_timetothreshold'
MYSQL_TABLE_GUD = 'e3_global_user_diff'

MYSQL_MILESTONES = """ select
                        bucket,
                        sum(if(edit_count >= 1,1,0)) as 1_edit,
                        sum(if(edit_count >= 5,1,0)) as five_edits,
                        sum(if(edit_count >= 10,1,0)) as 10_edits,
                        sum(if(edit_count >= 25,1,0)) as 25_edits,
                        sum(if(edit_count >= 50,1,0)) as 50_edits,
                        sum(if(edit_count >= 100,1,0)) as 100_edit
                        from e3_pef_iter2_bytesadded as a
                            join e3_pef_iter2_bucketed_users as b
                            on b.user_id = a.user_id
                        group by 1;
                    """

def main(args):
    """
        Data gathering for Post-Edit Feedback iteration #2
            - it is assumed that `staging`.`e3_pef_iter2_bucketed_users` exists
    """

    # initialize dataloader objects
    global data_loader
    data_loader = dl.Connector(instance='slave')

    global experiment_start_date
    experiment_start_date = datetime.datetime(year=2012,month=9,day=20)

    # Load eligible users
    logging.info('Retrieving namespace edits...')
    sql = 'select user_id from staging.%s' % MYSQL_TABLE_GUD
    eligible_users = dl.DataLoader().cast_elems_to_string(dl.DataLoader().get_elem_from_nested_list(
        data_loader.execute_SQL(sql),0))
    user_id_str = dl.DataLoader().format_comma_separated_list(eligible_users)
    logging.info('There are %s eligible users.  Processing ...' % len(eligible_users))

    # PROCESS BLOCKS
    if args.blocks_table:
        get_blocks()
        # create index user_id_idx on e3_pef_iter2_blocks (user_id);

    if args.global_user_diff_table:
        data_loader.execute_SQL('drop table if exists %s' % MYSQL_TABLE_GUD)
        sql = dl.read_file(settings.__sql_home__ + 'create_%s.sql' % MYSQL_TABLE_GUD)
        data_loader.execute_SQL(sql)
        # create index user_id_idx on e3_global_user_diff (user_id);

    # Create a table storing edits by namespace
    if args.ns_table:
        logging.info('Retrieving namespace edits...')
        data_loader.execute_SQL('drop table if exists %s' % MYSQL_TABLE_NAMESPACE)
        sql = """
                create table %(ns_table)s
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
            """ % {
            'users' : user_id_str,
            'ns_table' : MYSQL_TABLE_NAMESPACE }
        data_loader.execute_SQL(" ".join(sql.strip().split()))

    # Get eligible users
    if args.filter:
        eligible_users = filter_users()

    # PROCESS TIME TO THRESHOLD
    if args.bytes_added_table:
        get_bytes_added(eligible_users)

    # PROCESS TIME TO THRESHOLD
    if args.time_to_threshold_table:
        # get_time_to_threshold(eligible_users)
        # get_time_to_threshold(eligible_users,outfile='e3_pef_iter2_survival_bucket.tsv',threshold_edit=5)
        get_time_to_threshold(eligible_users,outfile='e3_pef_iter2_ttt_5_10.tsv',first_edit=5, threshold_edit=10)
        get_time_to_threshold(eligible_users,outfile='e3_pef_iter2_ttt_10_25.tsv',first_edit=10, threshold_edit=25)
        get_time_to_threshold(eligible_users,outfile='e3_pef_iter2_ttt_25_50.tsv',first_edit=25, threshold_edit=50)
        get_time_to_threshold(eligible_users,outfile='e3_pef_iter2_ttt_50_100.tsv',first_edit=50, threshold_edit=100)

    # PROCESS REVERTS
    if args.reverts:
        get_reverts(eligible_users)

def get_reverts(eligible_users):
    """ Genereate user reverts """

    global experiment_start_date

    logging.info("Processing revert rate for %s users." % str(len(eligible_users)))
    sql_reg_date = 'select user_registration from enwiki.user where user_id = %s;'

    reverts = []
    bad_users = 0
    logging.info('Processing %s eligible users...' % str(len(eligible_users)))

    for user in eligible_users:
        reg_date = date_parse(data_loader.execute_SQL(sql_reg_date % user)[0][0])
        end_date = reg_date + datetime.timedelta(days=14)

        try:
            entry = r.RevertRate(date_start=reg_date,date_end=end_date).process(
                user).__iter__().next()
            reverts.append(entry)

        except Exception as e:
            bad_users += 1

    logging.info('Missed %s users out of %s.' % (str(bad_users), str(len(eligible_users))))
    logging.info('Writing results to table.')
    dl.DataLoader().list_to_xsv(reverts)

    # Create table
    sql = """
        CREATE TABLE `e3_pef_iter2_revertrate` (
      `user_id` int(5) unsigned NOT NULL DEFAULT 0,
      `revert_rate` varbinary(255) NOT NULL DEFAULT '',
      `total_revs` varbinary(255) NOT NULL DEFAULT ''
    ) ENGINE=MyISAM DEFAULT CHARSET=binary
    """

    data_loader.execute_SQL('drop table if exists %s' % MYSQL_TABLE_REVERTS)
    dl.DataLoader().create_table_from_xsv('list_to_xsv.out', " ".join(sql.strip().split()),
        MYSQL_TABLE_REVERTS, header=False)

    sql = """
                select
                    r.user_id,
                    d.bucket,
                    r.revert_rate,
                    r.tital_revs
                from %(ba_table)s as r
                    join %(buckets_table)s as d
                    on d.user_id = r.user_id
            """ % {
        'ba_table' : MYSQL_TABLE_REVERTS,
        'buckets_table' : MYSQL_TABLE_BUCKETS}
    dl.DataLoader().create_xsv_from_SQL(" ".join(sql.strip().split()), outfile = 'e3_pef_iter2_revert_bucket.tsv')

def get_blocks():
    """ Process blocks """
    global experiment_start_date
    logging.info("Processing blocked users for %s" % str(experiment_start_date))
    sql = """
            select
                r.user_id,
                user.user_name
            from (  select
                        distinct user_id as user_id
                    from %(buckets_table)s) as r
            join enwiki.user on r.user_id = user.user_id
          """ % {'buckets_table' : MYSQL_TABLE_BUCKETS}
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

    eligible_user_names = dl.DataLoader().cast_elems_to_string(dl.DataLoader().get_elem_from_nested_list(results,1))
    block_list = b.Blocks(date_start=str(experiment_start_date)).process(eligible_user_names)._results

    # Replace user names with IDs
    for i in xrange(len(block_list)):
        try:
            block_list[i][0] = str(h[block_list[i][0]])
        except KeyError:
            logging.error('Cannot include %s in result.' % block_list[i][0])
            pass

    dl.DataLoader().list_to_xsv(block_list)
    data_loader.execute_SQL('drop table if exists %s' % MYSQL_TABLE_BLOCKS)
    sql = dl.read_file(settings.__sql_home__ + 'create_e3_pef_iter2_blocks.sql')
    dl.DataLoader().create_table_from_xsv('list_to_xsv.out', sql, MYSQL_TABLE_BLOCKS)

    sql = """
                select
                    r.user_id,
                    d.bucket,
                    r.block_count,
                    r.first_block,
                    r.last_block,
                    r.ban
                from %(blocks_table)s as r
                    join %(buckets_table)s as d
                    on d.user_id = r.user_id
            """ % {
        'blocks_table' : MYSQL_TABLE_BLOCKS,
        'buckets_table' : MYSQL_TABLE_BUCKETS}
    dl.DataLoader().create_xsv_from_SQL(" ".join(sql.strip().split()), outfile = 'e3_pef_iter2_blocks_bucket.tsv')

def get_bytes_added(eligible_users):
    """ process "bytes added" """
    global experiment_start_date

    logging.info("Processing bytes added for %s users." % str(len(eligible_users)))
    sql_reg_date = 'select user_registration from enwiki.user where user_id = %s;'

    bytes_added = list()
    bad_users = 0
    logging.info('Processing %s eligible users...' % str(len(eligible_users)))

    for user in eligible_users:
        try:
            reg_date = date_parse(data_loader.execute_SQL(sql_reg_date % user)[0][0])
            end_date = reg_date + datetime.timedelta(days=14)
            entry = ba.BytesAdded(date_start=reg_date, date_end=end_date).process([user]).__iter__().next()
            # entry.append(str((reg_date-experiment_start_date).seconds / 3600))
            bytes_added.append(entry)

        except Exception as e:
            logging.error('Could not get bytes added for user %s: %s' % (str(user), e.message))
            bad_users += 1

    logging.info('Missed %s users out of %s.' % (str(bad_users), str(len(eligible_users))))
    logging.info('Writing results to table.')
    dl.DataLoader().list_to_xsv(bytes_added)

    # Create table
    data_loader.execute_SQL('drop table if exists %s' % MYSQL_TABLE_BYTES_ADDED)
    sql = dl.read_file(settings.__sql_home__ + 'create_e3_pef_iter2_bytes_added.sql')
    dl.DataLoader().create_table_from_xsv('list_to_xsv.out', sql, MYSQL_TABLE_BYTES_ADDED, header=False)

    sql = """
                select
                    r.user_id,
                    d.bucket,
                    r.bytes_added_net,
                    r.bytes_added_abs,
                    r.bytes_added_pos,
                    r.bytes_added_net,
                    r.edit_count
                from %(ba_table)s as r
                    join %(buckets_table)s as d
                    on d.user_id = r.user_id
            """ % {
        'ba_table' : MYSQL_TABLE_BYTES_ADDED,
        'buckets_table' : MYSQL_TABLE_BUCKETS}
    dl.DataLoader().create_xsv_from_SQL(" ".join(sql.strip().split()), outfile = 'e3_pef_iter2_ba_bucket.tsv')

def get_time_to_threshold(eligible_users, first_edit=1, threshold_edit=2,outfile='e3_pef_iter2_ttt_bucket.tsv'):
    """ Time to Threshold metric processing """
    logging.info("Processing time to threshold for %s users." % str(len(eligible_users)))

    # create table
    sql = dl.read_file(settings.__sql_home__ + 'create_e3_pef_iter2_timetothreshold.sql')
    t = ttt.TimeToThreshold(ttt.TimeToThreshold.EditCountThreshold,
        first_edit=first_edit, threshold_edit=threshold_edit).process(eligible_users)

    dl.DataLoader().list_to_xsv(t)
    data_loader.execute_SQL('drop table if exists %s' % MYSQL_TABLE_TTT)
    dl.DataLoader().create_table_from_xsv('list_to_xsv.out', sql, MYSQL_TABLE_TTT, header=False)

    sql = """
            select
                r.user_id,
                d.bucket,
                r.time_minutes
            from %(ttt_table)s as r
                join %(buckets_table)s as d on d.user_id = r.user_id
        """ % {
        'ttt_table' : MYSQL_TABLE_TTT,
        'buckets_table' : MYSQL_TABLE_BUCKETS}
    dl.DataLoader().create_xsv_from_SQL(" ".join(sql.strip().split()),outfile=outfile)

def filter_users():
    """  """
    logging.info('Retrieving eligible users filtered by blocks and global/local user registration...')
    sql = """
                    select
                    distinct rev_user
                    from %(ns_table)s as ns
                        join %(blocks_table)s as b
                            on ns.rev_user = b.user_id
                        join %(gud_table)s as gu
                            on gu.user_id = b.user_id
                    where block_count = 0 and delta >= -7 and ns.revisions > 0
                    """ % {
        'ns_table' : MYSQL_TABLE_NAMESPACE,
        'blocks_table' : MYSQL_TABLE_BLOCKS,
        'gud_table' : MYSQL_TABLE_GUD}
    eligible_users = dl.DataLoader().cast_elems_to_string(dl.DataLoader().get_elem_from_nested_list(
        data_loader.execute_SQL(sql),0))
    logging.info('There are %s eligible users after filtering blocks, attached users, '
                 'and 0 revisions.' % len(eligible_users))

    eligible_users = df.filter_bots(eligible_users)
    logging.info('There are %s eligible users after filtering bots.' % len(eligible_users))

    # Write filtered users to file
    f_obj = open('e3_pef_iter2_filtered.tsv', 'w')
    for u in eligible_users:
        f_obj.write(u + '\n')
    f_obj.close()

    return eligible_users

# Call Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script for generating metrics from Post-Edit Feedback iteration 2 Experiment.  " \
                    "Relies on a connection to an enwiki database.  Expects the existence of a " \
                    "table containing bucketed users from the experiment.",
        epilog="",
        conflict_handler="resolve"
    )
    parser.add_argument('-f', '--filter',action="store_true",help='Filter eligible users.',default=False)
    parser.add_argument('-n', '--ns_table',action="store_true",help='Process namespace edits.',default=False)
    parser.add_argument('-b', '--blocks_table',action="store_true",help='Process blocks.',default=False)
    parser.add_argument('-y', '--bytes_added_table',action="store_true",help='Process bytes added metric',default=False)
    parser.add_argument('-t', '--time_to_threshold_table',action="store_true",
        help='process time to threshold metric.',
        default=False)
    parser.add_argument('-g', '--global_user_diff_table',action="store_true",
        help='Process globaluser timestamp filter.',
        default=False)
    parser.add_argument('-r', '--reverts',action="store_true",help='Process revert rate metric',default=False)
    args = parser.parse_args()
    logging.info('Arguments: %s' % args)
    sys.exit(main(args))


