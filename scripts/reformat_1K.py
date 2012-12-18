#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
	reformat_1k.py - special purpose script for reformatting tsv columns

	Usage:

	    python reformat_1k.py [OPTIONS]


	Example:

	    RFaulkner-WMF:projects rfaulkner$  python reformat_1k.py -i ./data/1k_editors_by_day.tsv -o my_output.tsv

        RFaulkner-WMF:projects rfaulkner$ tail -n 10 my_output.tsv
        Zagmac	2006-08-20	25	0	0	0	0	0	0
        Zamyusoff	2010-04-25	3	0	0	0	0	0	0
        ZappaOMati	2012-02-28	8	8	23	16	28	29	29
        Zedshort	2011-10-21	1	0	0	0	0	0	0
        Zepppep	2009-07-31	2	0	0	0	0	0	0
        Zhinz	2005-11-23	1	0	0	0	0	0	0
        Zimmygirl7	2011-06-28	4	0	0	0	2	4	3
        Zujua	2011-11-03	21	0	1	0	0	12	0
        Érico Júnior Wouters	2010-05-08	2	0	0	0	0	0	0
        Свифт	2009-05-17	1	0	0	0	0	0	0

"""

__author__ = "Ryan Faulkner <rfaulkner@wikimedia.org>"
__license__ = "GPL (version 2 or later)"

import sys
import logging
import datetime
import argparse
from dateutil.parser import *

global RECS
RECS=7

# CONFIGURE THE LOGGER
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

def main(args):

    parser = argparse.ArgumentParser(
        description="Processes csv files composed of editor IDs, names, and, email addresses.",
        epilog="python revision -f pt-2010-all.csv -s revids_of_pt_editors_weve_emailed.csv -e pt_users_email.tsv -a pt_users_2010.tsv -o parsed_users.tsv -w True",
        conflict_handler="resolve"
    )
    parser.add_argument('-i', '--input',type=str, help='Input tsv filename.',default="./input.tsv")
    parser.add_argument('-o', '--output',type=str, help='Output tsv filename.',default="./output.tsv")

    args = parser.parse_args()

    infile = open(args.input, 'r')
    outfile = open(args.output, 'w')

    new_row = list()
    recs_added = RECS

    logging.info('Parsing %(infile)s to %(outfile)s' % {'infile' : args.input, 'outfile' : args.output})

    line = infile.readline()
    curr_user = None

    while (line):

        elems = line.split('\t')

        user = elems[0]
        day = parse(elems[1])

        # Check if we need to look at a new user
        if recs_added == RECS or user != curr_user:

            if recs_added < RECS:
                for i in range(RECS - recs_added):
                    new_row.append('0')

            # ignore remaining lines for this user
            else:
                while curr_user == user:
                    line = infile.readline()

                    if not(line):
                        break

                    elems = line.split('\t')
                    user = elems[0]

            # Write the new row to the outfile
            outfile.write("\t".join(new_row) + '\n')

            new_row = list()

            curr_user = elems[0]
            curr_day = parse(elems[1])

            new_row.append(curr_user)
            new_row.append(elems[1])
            new_row.append(elems[2].strip())

            recs_added = 1

        # If the next record sequentially follows from the previous add the
        curr_day += datetime.timedelta(days=1)
        if day == curr_day:
            new_row.append(elems[2].strip())
            recs_added += 1

        # If there are missing
        while day > curr_day and recs_added < RECS:
            new_row.append('0')
            curr_day += datetime.timedelta(days=1)
            recs_added += 1

        line = infile.readline()

    logging.info('Transform complete.')

    # close files
    infile.close()
    outfile.close()

# Call Main
if __name__ == "__main__":
    sys.exit(main([]))
