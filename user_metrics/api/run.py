#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    This module defines the entry point for flask_ web server implementation
    of the Wikimedia User Metrics API.  This module is consumable
    by the Apache web server via WSGI interface via mod_wsgi.  An Apache
    server can be pointed to api.wsgi such that Apache may be used as a
    wrapper in this way.

    .. _flask: http://flask.pocoo.org

    Cohort Data
    ^^^^^^^^^^^

    Cohort data is maintained in the host s1-analytics-slave.eqiad.wmnet under
    the `staging` database in the `usertags` and `usertags_meta` tables: ::

        +---------+-----------------+------+-----+---------+-------+
        | Field   | Type            | Null | Key | Default | Extra |
        +---------+-----------------+------+-----+---------+-------+
        | ut_user | int(5) unsigned | NO   | PRI | NULL    |       |
        | ut_tag  | int(4) unsigned | NO   | PRI | NULL    |       |
        +---------+-----------------+------+-----+---------+-------+

        +-------------+-----------------+------+-----+---------+
        | Field       | Type            | Null | Key | Default |
        +-------------+-----------------+------+-----+---------+
        | utm_id      | int(5) unsigned | NO   | PRI | NULL    |
        | utm_name    | varchar(255)    | NO   |     |         |
        | utm_notes   | varchar(255)    | YES  |     | NULL    |
        | utm_touched | datetime        | YES  |     | NULL    |
        +-------------+-----------------+------+-----+---------+


"""

__author__ = {
    "dario taraborelli": "dario@wikimedia.org",
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2012-12-21"
__license__ = "GPL (version 2 or later)"



import cPickle
from user_metrics.config import logging
import os

import user_metrics.config.settings as settings
import multiprocessing as mp
from collections import OrderedDict
from shutil import copyfile

from engine.request_manager import job_control
from user_metrics.api.views import app



######
#
# Define Custom Classes
#
#######


class APIMethods(object):
    """ Provides initialization and boilerplate for API execution """

    __instance = None   # Singleton instance
    __job_controller_proc = None

    def __new__(cls):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(APIMethods, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        """ Load cached data from pickle file. """
        global request_queue
        global response_queue

        # Setup the job controller
        if not self.__job_controller_proc:
            self._setup_controller(request_queue, response_queue)

        # Open the pickled data for reading.
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'rb')
        except IOError:
            pkl_file = None

        # test whether the open was successful
        if pkl_file:
            try:
                pkl_data = cPickle.load(pkl_file)
            except ValueError:
                # Generally due to a "insecure string pickle"
                logging.error(__name__ + ':: Could not access pickle data.')
                pkl_data = OrderedDict()

                # Move the bad pickle data into a new file and recreate the
                # original as an empty file
                src = settings.__data_file_dir__ + 'api_data.pkl'
                dest = settings.__data_file_dir__ + 'api_data.pkl.bad'

                copyfile(src, dest)
                os.remove(src)
                with open(src, 'wb'):
                    pass

            pkl_file.close()

    def close(self):
        """ When the instance is deleted store the pickled data and shutdown
            the job controller """
        global pkl_data

        # Handle persisting data to file
        pkl_file = None
        try:
            pkl_file = open(settings.__data_file_dir__ + 'api_data.pkl', 'wb')
            cPickle.dump(pkl_data, pkl_file)
        except Exception:
            logging.error(__name__ + '::Could not pickle data.')
        finally:
            if hasattr(pkl_file, 'close'):
                pkl_file.close()

        # Try to shutdown the job control proc gracefully
        try:
            if self.__job_controller_proc and\
               hasattr(self.__job_controller_proc, 'is_alive') and\
               self.__job_controller_proc.is_alive():
                self.__job_controller_proc.terminate()
        except Exception:
            logging.error(__name__ + ' :: Could not shut down controller.')

    def _setup_controller(self, req_queue, res_queue):
        """
            Sets up the process that handles API jobs
        """
        self.__job_controller_proc = mp.Process(target=job_control,
                                                args=(req_queue,res_queue))
        if not self.__job_controller_proc.is_alive():
            self.__job_controller_proc.start()


######
#
# Execution
#
#######


if __name__ == '__main__':

    # initialize API data - get the instance
    a = APIMethods()
    try:
        app.run(debug=True)
    finally:
        a.close()
