"""

"""

__author__ = "Ryan Faulkner"
__date__ = "November 9th, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import cgi
from urlparse import urlparse
import re
import logging

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class LineParseMethods():
    """
        Helper Class - Defines methods for processing lines of text primarily from log files.  Each method in this class takes one argument:

            - **line** - String.  Line text to process.

        The return value of the method is simply some function of the input defined by the transformation method.
    """

    def e3lm_log_parse(self, line):
        """
            Data Format:

                https://meta.wikimedia.org/wiki/Research:Timestamp_position_modification/Clicktracking

            e.g. from /var/log/aft/click-tracking.log ::
                enwiki ext.lastModified@1-ctrl1-impression	20120622065341	0	aLIoSWm5H8W5C91MTT4ddkHXr42EmTxvL	0	0	0	0	0

        """
        elems = line.split('\t')
        l = elems[0].split()
        l.extend(elems[1:])

        # in most cases the additional data will be missing - append a field here
        if len(l) < 11:
            l.append("no data")
        return l

    def e3_pef_log_parse(self, line):
        """
            Data Format:

                https://meta.wikimedia.org/wiki/Research:Timestamp_position_modification/Clicktracking

            e.g. from /var/log/aft/click-tracking.log ::
                enwiki ext.postEditFeedback@1-assignment-control	20120731063615	1	FGiANxyrmVcI5InN0myNeHabMbPUKQMCo	0	0	0	0	0	15667009:501626433
        """
        elems = line.split('\t')

        page_id = ''
        rev_id = ''
        user_hash = ''

        try:
            additional_data = elems[9]
            additional_data_fields = additional_data.split(':')

            if len(additional_data_fields) == 2:
                page_id = additional_data_fields[0]
                rev_id = additional_data_fields[1]
                user_hash = ''

            elif len(additional_data_fields) == 3:
                page_id = additional_data_fields[0]
                rev_id = additional_data_fields[1]
                user_hash = additional_data_fields[2]

        except:
            logging.info('No additional data for event %s at time %s.' % (elems[0], elems[1]))

        l = elems[0].split()
        l.extend(elems[1:9])

        l.append(user_hash)
        l.append(rev_id)
        l.append(page_id)

        # Append fields corresponding to `e3pef_time_to_milestone` and `e3pef_revision_measure`
        l.extend(['',''])

        return l

    def e3_acux_log_parse(self, line):
        """
            Process client and server side events.  Read to table, gather clean funnels.

            Dario says:

                enwiki ext.accountCreationUX@2-acux_1-assignment	20121005000446	0	hGba7rOPWNmpc9lA7EQLnB5Nvb7ziBqoT	-1	0	0	0	0	frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC|http://en.wikipedia.org/w/index.php?title=Special:UserLogin&returnto=Miguel_(singer)&returntoquery=action%3Dedit
                enwiki ext.accountCreationUX@2-acux_1-impression	20121005000447	0	hGba7rOPWNmpc9lA7EQLnB5Nvb7ziBqoT	-1	0	0	0	0	frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC|http://en.wikipedia.org/w/index.php?title=Special:UserLogin&returnto=Miguel_(singer)&returntoquery=action%3Dedit
                enwiki ext.accountCreationUX@2-acux_1-submit	20121005000508	0	hGba7rOPWNmpc9lA7EQLnB5Nvb7ziBqoT	-1	0	0	0	0	frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC|http://en.wikipedia.org/w/index.php?title=Special:UserLogin&returnto=Miguel_(singer)&returntoquery=action%3Dedit|Mariellaknaus
                enwiki ?event_id=account_create&user_id=17637802&timestamp=1349395510&username=Mariellaknaus&self_made=1&creator_user_id=17637802&by_email=0&userbuckets=%7B%22ACUX%22%3A%5B%22acux_1%22%2C2%5D%7D&mw_user_token=frGBqVHW1eAHGQwldL8dhFs3R8ocZ9TC&version=2

                The sequence of events in this "clean" funnel is the following:

                acux_1-assignment 	(client event)
                acux_1-impression 	(client event)
                acux_1-submit 		(client event)
                account_create 		(server event)

                The full specs of the events are here: https://meta.wikimedia.org/wiki/Research:Account_creation_UX/Logging

                Duplicate events
                Since users can go through complex funnels before submitting the account create form and generate errors after submitting, "clean"
                funnels are going to be the exception, not the norm and we will need to collapse all funnels by token to extract meaningful metrics.
                In other words, raw counts of -impression or -submit events will be meaningless and should not be used to calculate click through/conversion
                rates prior to deduplication.

                As a rule, there should only be one (server-side) account_create event associated with a token. The only exception is shared browsers
                creating multiple accounts. In this case we will see multiple account_create events associated with the same token (which is persistent
                 across sessions and logins) but different user_id's.

                Early stats
                In the first hour since activation (23.30-00.30 UTC), we had 87 successful account creations from the acux_1 bucket vs 74
                accounts from the control. This doesn't include users who by-passed the experiment by having JS disabled. The total number of
                accounts registered in this hour on enwiki, per the logging table, is 180, so users who didn't get bucketed are 19 (i.e.
                about 10% of all account registrations). This figure is higher than I expected so there might be other causes on top of JS
                disabled that cause users to register without a bucket and that we may want to investigate.
        """
        line_bits = line.split('\t')
        num_fields = len(line_bits)

        # handle both events generated from the server and client side via ACUX.  Discriminate the two cases based
        # on the number of fields in the log

        if num_fields == 1:
            # SERVER EVENT - account creation
            line_bits = line.split()
            query_vars = cgi.parse_qs(line_bits[1])

            try:
                # Ensure that the user is self made
                if query_vars['self_made'][0]:
                    return [line_bits[0], query_vars['username'][0], query_vars['user_id'][0],
                            query_vars['timestamp'][0], query_vars['?event_id'][0], query_vars['self_made'][0],
                            query_vars['mw_user_token'][0], query_vars['version'][0], query_vars['by_email'][0],
                            query_vars['creator_user_id'][0]]
                else:
                    return []

            except Exception:
                return []

        elif num_fields == 10:
            # CLIENT EVENT - impression, assignment, and submit events
            fields = line_bits[0].split()
            fields.extend(line_bits[1:9])
            additional_fields = ['','']
            last_field = line_bits[9].split('|')

            if len(last_field) >= 2:
                additional_fields[0] = last_field[0]
                additional_fields[1] = last_field[1]

            elif len(last_field) == 1:
                # Check whether the additional fields contain only a url
                if urlparse(last_field[0]).scheme:
                    additional_fields[1] = last_field[0]
                else:
                    additional_fields[0] = last_field[0]
            fields.extend(additional_fields)
            return fields
        return []

    @staticmethod
    def e3_cta4_log_parse(line):
        """ Parse logs for AFT5-CTA4 log requests """

        line_bits = line.split('\t')
        num_fields = len(line_bits)

        regex_1 = r"ext.articleFeedbackv5@10-option6X-cta_signup_login-impression"
        regex_2 = r"ext.articleFeedbackv5@10-option6X-cta_signup_login-button_signup_click"

        if num_fields == 10 and (re.search(regex_1, line) or re.search(regex_2, line)):

            fields = line_bits[0].split()
            if re.search(regex_1, line):
                fields.append('impression')
            else:
                fields.append('click')

            fields.append(line_bits[1])
            fields.append(line_bits[3])
            last_field = line_bits[9].split('|')
            if len(last_field) == 3:
                fields.extend([i.strip() for i in last_field])
            else:
                return []
            return fields
        return []