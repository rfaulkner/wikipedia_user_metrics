__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2012"

import sys
import MySQLdb
import re
import logging
import numpy as np
import data_loader as dl

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')

class ExperimentsLoader(dl.DataLoader):
    """
        This class contains methods and classes for retrieving and processing experimental data.
    """

    def __init__(self):
        """
            Call constructor of parent
        """
        dl.DataLoader.__init__(self, db='slave')

    def get_user_email(self, users):
        """
            Get user emails from a list of users.  Queries the *user* table.  Returns a list of tuples containing user name and email.

            Parameters:
                - **users** - list of user names for whom to retrieve email addresses (where available)

            Return:
                - List(tuple).  Query results.
        """

        logging.info('Finding user emails')

        value = ','.join('"???"'.join(users).split('???')[1:-2])
        sql = 'select user_name, user_email from user where user_name in (%s) and user_email_authenticated IS NOT NULL;'
        sql = sql % value

        return self.execute_SQL(sql)


    def filter_by_edit_counts(self, rows, index, lower_threshold):
        """
            Filters a list of users by edit count.  Returns the rows meeting the minimum threshold criteria.

            Parameters:
                - **rows** - list of tuples containing user data (SQL output)
                - **index** - index of edit count
                - **lower_threshold** - minimum value of edit count

            Return:
                - List(tuple).  Filtered results.
        """

        new_rows = list()
        for row in rows:
            if int(row[index]) >= lower_threshold:
                new_rows.append(row)

        return new_rows


    def get_user_revisions(self, users, start_time, end_time):
        """
            Select all revisions in a given time period from the revision table for the given list of users.  Queries the *revision* table.
            Returns two lists, the query fields and the results of the query.

            Parameters:
                - **users** - list of users on which to condition the query
                - **start_time** - earliest revision timestamp
                - **end_time** - latest revision timestamp

            Return:
                - List(string).  Column names.
                - List(tuple).  Query results.
        """

        users_str = self.format_comma_separated_list(users)

        sql = 'select rev_timestamp, rev_comment, rev_user, rev_user_text, rev_page from revision ' +\
              'where rev_timestamp >= "%(start_timestamp)s" and rev_timestamp < "%(end_timestamp)s" and rev_user_text in (%(users)s)'
        sql = sql % {'start_timestamp' : start_time, 'end_timestamp' : end_time, 'users' : users_str}

        self._results_ = self.execute_SQL(sql)
        col_names = self.get_column_names()

        return col_names, self._results_


    def get_user_page_ids(self, filename, parse_method, header=True):
        """
            Get a list of user page ids embedded in a text file.  A parse method is used to extract the ids from each line of the input file.
            This method queries the page table conditioned on UserTalk namespace and the user list generated from the file.

            Parameters:
                - **filename** - String.  The .xsv filename, assumed to be located in the project data folder.
                - **parse_method** - Method Pointer.  Method that handles extracting content from each line (see helper class XSVParseMethods).
                - **header** - Boolean.  Flag indicating whether the file has a header.

            Return:
                - List.  List of elements parsed from each line of the input.
        """

        try:

            users = self.extract_pattern_from_text_file(filename, parse_method, header=header)
            user_list_str = self.format_comma_separated_list(users)

            # get page ids for eligible - format csv list
            sql = "select page_id from page where page_namespace = 3 and page_title in (%s);" % user_list_str

            result = self.execute_SQL(sql)

            id_list = list()
            for row in result:
                id_list.append(str(row[0]))

        except Exception():
            logging.error('Could not parse ids from: %s' % filename)
            return []

        return id_list
        # user_list_str = self.format_comma_separated_list(id_list)
        # return "".join(["rev_page in (", user_list_str, ")"])


    def write_sample_aggregates(self, samples, buckets, bin_values, bins=None, interval=1):
        """
            Takes a set of samples and corresponding buckets and builds a dictionary (which is written to a file) containing binned
            samples and the parameters for each bin by treatment

            Parameters:
                - **samples** - List(numeric).  The sample values.
                - **buckets** - List(String).  The treatments fro each sample.
                - **bins** - List(numeric).  list of bin breaks - defaults to None.
                - **interval** - Integer.  Interval length between bins - ignored in 'bins' is defined.

            Return:
                - Dict(list).  The dictionary composed from the samples.
        """

        # Input verification
        samples = list(samples)
        buckets = list(buckets)
        bin_values = list(bin_values)

        if not(len(samples) == len(buckets) and len(bin_values) == len(buckets)):
            raise Exception('Samples, bin values and buckets must be of the same length.')

        # Set the bins
        if bins == None:
            bins = np.array(range(min(bin_values), max(bin_values) + 1, interval))
        else:
            bins = np.array(bins).sort()

        logging.info('The bin break values are: %s' % str(bins))

        treatments = list(set(buckets)) # get the unique
        all_samples = dict()
        out_keys = list()

        # format
        for i in range(len(treatments)):
            effect_index = ''.join(['y',str(i)])
            out_keys.append(effect_index)
            out_keys.append(''.join(['bucket',str(i)]))
            out_keys.append(''.join([effect_index, '_mean']))
            out_keys.append(''.join([effect_index, '_sd']))
            out_keys.append(''.join([effect_index, '_min']))
            out_keys.append(''.join([effect_index, '_max']))

            # Initialize dict to store all samples by treatment and bin
            all_samples[effect_index] = list()
            for j in range(len(bins)):
                all_samples[effect_index].append(list())

        out_keys = np.array(out_keys)
        ret = dict()
        for key in out_keys:
            ret[key] = [0] * len(bins)

        ret['x'] = list(bins + interval / 2.0)

        # Pass through the sample list to extract samples and counts
        bad_bin = 0
        for i in range(len(samples)):
            s = samples[i]  # sample value
            b = buckets[i]  # sample treatment
            b_i = treatments.index(b) # index of sample treatment

            # find bin
            try:
                bin_index = list(bins >= s).index(True)

                if bin_index < 0:
                    bad_bin += 1
                    continue
            except ValueError:
                bad_bin += 1
                continue

            index = ''.join(['y',str(b_i)])
            if index in all_samples.keys():
                all_samples[index][bin_index].append(s)

        # report the number of "bad bins" - that is samples that fell outside of any bin
        logging.info('There were %s un-"binned" samples out of %s.' % (bad_bin, len(samples)))

        # Compute parameters
        for i in range(len(treatments)):

            sub_keys = list()
            for key in out_keys:
                if key.find(str(i)) >= 0:
                    sub_keys.append(key)

            for bin_index in range(len(bins)):
                sample_subset = all_samples[sub_keys[0]][bin_index]

                ret[sub_keys[0]][bin_index] = len(sample_subset)
                ret[sub_keys[1]][bin_index] = buckets[i]
                ret[sub_keys[2]][bin_index] = np.mean(sample_subset)
                ret[sub_keys[3]][bin_index] = np.std(sample_subset)

                if len(sample_subset) > 0:
                    ret[sub_keys[4]][bin_index] = np.min(sample_subset)
                else:
                    ret[sub_keys[4]][bin_index] = None

                if len(sample_subset) > 0:
                    ret[sub_keys[5]][bin_index] = np.max(sample_subset)
                else:
                    ret[sub_keys[5]][bin_index] = None


        # Write to xsv
        self.write_dict_to_xsv(ret)

        return ret