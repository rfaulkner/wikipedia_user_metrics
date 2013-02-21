"""
    Handles logic for parsing log requests into a readable format
"""

__author__ = "Ryan Faulkner"
__date__ = "November 9th, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import urlparse
import re
import logging
import json
import gzip
import user_metrics.config.settings as projSet

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class LineParseMethods():
    """
        Defines methods for processing lines of text primarily from log files.  Each method in this class takes one
        argument:

            - **line** - String.  Line text to process.

        The return value of the method is simply some function of the input defined by the transformation method.
    """

    @classmethod
    def parse(cls, log_file, parse_method, header=False, version=1):
        """
            Log processing wapper method.  This takes a log file as input and applies one of the parser methods to
            the contents, storing the list results in a list.
        """
        # Open the data file - Process the header
        if re.search('\.gz', log_file):
            file_obj = gzip.open(projSet.__data_file_dir__ + log_file, 'rb')
        else:
            file_obj = open(projSet.__data_file_dir__ + log_file, 'r')

        if header: file_obj.readline()

        contents = list()
        while 1:
            line = file_obj.readline()
            if not line: break
            contents.append(parse_method(line, version=version))
        return map(lambda x: x, contents)



    @staticmethod
    def e3_lm_log_parse(line, version=1):
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

    @staticmethod
    def e3_pef_log_parse(line, version=1):
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

        except IndexError:
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
    def e3_acux_log_parse_client_event(line, version=1):
        line_bits = line.strip().split('\t')
        num_fields = len(line_bits)

        regex_str = r'ext.accountCreationUX.*@.*_%s' % version

        if num_fields == 10 and re.search(regex_str, line):
            # CLIENT EVENT - impression, assignment, and submit events
            fields = line_bits[0].split()
            project = fields[0]
            event_desc = fields[1].split('@')[1].split('-')
            bucket = event_desc[1]
            event = event_desc[2]
            fields = [project, bucket, event]

            fields.extend(line_bits[1:5])

            additional_fields = ['None','None','None']
            parsed_add_fields = line_bits[9].split('|')

            for i in xrange(len(parsed_add_fields)):
                if i > 2: break
                additional_fields[i] = parsed_add_fields[i]
            fields.extend(additional_fields)
            return fields
        return []

    @staticmethod
    def e3_acux_log_parse_server_event(line, version=1):
        line_bits = line.split('\t')
        num_fields = len(line_bits)
        server_event_regex = r'account_create.*userbuckets.*ACUX'
        # handle both events generated from the server and client side via ACUX.  Discriminate the two cases based
        # on the number of fields in the log

        if num_fields == 1:
            # SERVER EVENT - account creation
            line_bits = line.split()

            try:
                if re.search(server_event_regex,line):
                    query_vars = urlparse.parse_qs(line_bits[1])
                    userbuckets = json.loads(query_vars['userbuckets'][0])

                    # Ensure that the user is self made, the event is account creation, and the version is correct
                    if query_vars['self_made'][0] and query_vars['?event_id'][0] == 'account_create' \
                    and str(version) in userbuckets['ACUX'][0]:

                        campaign = userbuckets['campaign'][0] if 'campaign' in userbuckets else ''

                        return [line_bits[0], query_vars['username'][0], query_vars['user_id'][0],
                                query_vars['timestamp'][0], query_vars['?event_id'][0], query_vars['self_made'][0],
                                query_vars['mw_user_token'][0], query_vars['version'][0], query_vars['by_email'][0],
                                query_vars['creator_user_id'][0], campaign]
                else:
                    return []

            except KeyError: return []
            except IndexError: return []
        return []

    @staticmethod
    def e3_cta4_log_parse_client(line, version=1):
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

    @staticmethod
    def e3_cta4_log_parse_server(line, version=1):
        """ Parse logs for AFT5-CTA4 log requests """

        line_bits = line.split('\t')
        num_fields = len(line_bits)

        if num_fields == 1:
            # SERVER EVENT - account creation
            line_bits = line.split()
            query_vars = urlparse.parse_qs(line_bits[1])

            try:
                # Ensure that the user is self made
                if query_vars['self_made'][0] and query_vars['?event_id'][0] == 'account_create' \
                and re.search(r'userbuckets',line) and 'campaign' in json.loads(query_vars['userbuckets'][0]):

                    return [line_bits[0], query_vars['username'][0], query_vars['user_id'][0],
                            query_vars['timestamp'][0], query_vars['?event_id'][0], query_vars['self_made'][0],
                            query_vars['version'][0], query_vars['by_email'][0], query_vars['creator_user_id'][0]]

                else:
                    return []

            except TypeError: return []
            except KeyError: return []
            except IndexError: return []
        return []