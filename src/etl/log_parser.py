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
        Defines methods for processing lines of text primarily from log files.  Each method in this class takes one
        argument:

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

    @staticmethod
    def e3_acux_log_parse_client_event(line, version=2):
        line_bits = line.split('\t')
        num_fields = len(line_bits)

        regex_str = r'ext.accountCreationUX.*acux_%s' % version

        if num_fields == 10 and re.search(regex_str, line):
            # CLIENT EVENT - impression, assignment, and submit events
            fields = line_bits[0].split()
            fields.extend(line_bits[1:5])
            additional_fields = ['','','']
            last_field = line_bits[9].split('|')

            if len(last_field) == 2:
                additional_fields[0] = str(last_field[0]).strip()
                additional_fields[1] = str(last_field[1]).strip()
                additional_fields[2] = str(last_field[2]).strip()

            elif len(last_field) == 2:
                additional_fields[0] = str(last_field[0]).strip()
                additional_fields[1] = str(last_field[1]).strip()

            elif len(last_field) == 1:
                # Check whether the additional fields contain only a url
                if urlparse(last_field[0]).scheme:
                    additional_fields[1] = str(last_field[0]).strip()
                else:
                    additional_fields[0] = str(last_field[0]).strip()
            fields.extend(additional_fields)
            return fields
        return []

    @staticmethod
    def e3_acux_log_parse_server_event(line, version=2):
        line_bits = line.split('\t')
        num_fields = len(line_bits)

        # handle both events generated from the server and client side via ACUX.  Discriminate the two cases based
        # on the number of fields in the log

        # ensure that event_id == `account_creation`
        # ensure that event_id == `account_creation`

        if num_fields == 1:
            # SERVER EVENT - account creation
            line_bits = line.split()
            query_vars = cgi.parse_qs(line_bits[1])

            try:
                # Ensure that the user is self made
                if query_vars['self_made'][0] and query_vars['?event_id'][0] == 'account_create':
                    return [line_bits[0], query_vars['username'][0], query_vars['user_id'][0],
                            query_vars['timestamp'][0], query_vars['?event_id'][0], query_vars['self_made'][0],
                            query_vars['mw_user_token'][0], query_vars['version'][0], query_vars['by_email'][0],
                            query_vars['creator_user_id'][0]]
                else:
                    return []

            except KeyError: return []
            except IndexError: return []
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