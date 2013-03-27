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


import multiprocessing as mp

from user_metrics.config import logging, settings
from user_metrics.api.engine.request_manager import job_control, \
    requests_notification_callback
from user_metrics.api.engine.response_handler import process_responses
from user_metrics.api.views import app
from user_metrics.api.engine.request_manager import api_request_queue, \
    req_notification_queue_out, req_notification_queue_in, api_response_queue
from user_metrics.utils import terminate_process_with_checks

job_controller_proc = None
response_controller_proc = None
rm_callback_proc = None


######
#
# Define Custom Classes
#
#######


def teardown():
    """ When the instance is deleted store the pickled data and shutdown
        the job controller """

    # Try to shutdown the job control proc gracefully
    try:
        terminate_process_with_checks(job_controller_proc)
        terminate_process_with_checks(response_controller_proc)
        terminate_process_with_checks(rm_callback_proc)

    except Exception:
        logging.error(__name__ + ' :: Could not shut down callbacks.')


def setup_controller(req_queue, res_queue, msg_queue_in, msg_queue_out):
    """
        Sets up the process that handles API jobs
    """
    job_controller_proc = mp.Process(target=job_control,
                                     args=(req_queue, res_queue))
    response_controller_proc = mp.Process(target=process_responses,
                                          args=(res_queue,
                                                msg_queue_in))
    rm_callback_proc = mp.Process(target=requests_notification_callback,
                                  args=(msg_queue_in,
                                        msg_queue_out))
    job_controller_proc.start()
    response_controller_proc.start()
    rm_callback_proc.start()

######
#
# Execution
#
#######

# initialize API data - get the instance

setup_controller(api_request_queue, api_response_queue,
                 req_notification_queue_in, req_notification_queue_out)

app.config['SECRET_KEY'] = settings.__secret_key__

# With the presence of flask.ext.login module
if settings.__flask_login_exists__:
    from user_metrics.api.session import login_manager
    login_manager.setup_app(app)


if __name__ == '__main__':
    try:
        app.run(debug=True,
                use_reloader=False,
                host=settings.__instance_host__,
                port=settings.__instance_port__,)
    finally:
        teardown()
