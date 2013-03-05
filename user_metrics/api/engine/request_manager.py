"""
    This module implements the request manager functionality.
"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2013-03-05"
__license__ = "GPL (version 2 or later)"

from flask import jsonify, make_response

from user_metrics.config import logging
from user_metrics.api.engine import MAX_CONCURRENT_JOBS, \
    QUEUE_WAIT, MW_UID_REGEX, get_users
from user_metrics.api.engine.request_meta import QUERY_PARAMS_BY_METRIC
from user_metrics.api.engine.request_meta import RequestMetaFactory
from user_metrics.metrics.users import MediaWikiUser
from user_metrics.metrics.metrics_manager import process_data_request

from multiprocessing import Process, Queue
from json import loads
from collections import namedtuple
from re import search
from os import getpid

job_item_type = namedtuple('JobItem', 'id process request queue')


def job_control(request_queue, response_queue):
    """
        Controls the execution of user metrics requests

        Parameters
        ~~~~~~~~~~

        request_queue : multiprocessing.Queue
           Queues incoming API requests.

        response_queue : multiprocessing.Queue
           Queues processed responses.
    """

    # Store executed and pending jobs respectively
    job_queue = list()
    wait_queue = list()

    # Global job ID number
    job_id = 0

    # Tallies the number of concurrently running jobs
    concurrent_jobs = 0

    logging.debug('{0} :: {1}  - STARTING...'
    .format(__name__, job_control.__name__))

    while 1:

        # Request Queue Processing
        # ------------------------

        try:
            # Pull an item off of the queue
            req_item = request_queue.get(timeout=QUEUE_WAIT)

            logging.debug('{0} :: {1}  Pull item from request queue -> {2}'
            .format(__name__, job_control.__name__, str(req_item)))

        except Exception:
            req_item = None
            logging.debug('{0} :: {1}  - Listening ...'
            .format(__name__, job_control.__name__))


        # Process complete jobs
        # ---------------------

        for job_item in job_queue:
            # Look for completed jobs
            if not job_item.process.is_alive():

                # Pull data off of the queue and add it to the queue data
                queue_data = loads(job_item.queue.get().data)
                response = make_response(jsonify(queue_data))

                response_queue.put(response)
                del job_queue[job_queue.index(job_item)]

                concurrent_jobs -= 1

                logging.debug('{0} :: {1}  - RUN -> RESPONSE {2}'
                .format(__name__, job_control.__name__, str(job_item)))


        # Process pending jobs
        # --------------------

        for wait_req in wait_queue:
            if concurrent_jobs <= MAX_CONCURRENT_JOBS:
                # prepare job from item

                # req_q = Queue()
                # proc = Process(target=process_metrics, args=(req_q, wait_req))
                # proc.start()
                #
                # job_item = job_item_type(job_id, proc, wait_req, req_q)
                # job_queue.append(job_item)

                job_queue.append(wait_req)

                concurrent_jobs += 1
                job_id += 1

                logging.debug('{0} :: {1}  - WAIT -> RUN {2}'
                .format(__name__, job_control.__name__, str(wait_req)))


        # Add newest job to the queue
        # ---------------------------

        if req_item and concurrent_jobs <= MAX_CONCURRENT_JOBS:
            rm = RequestMetaFactory(req_item['cohort'],
                                    req_item['cohort_refresh_ts'],
                                    req_item['metric'])
            logging.debug('{0} :: {1}  - REQUEST -> WAIT {2}'
            .format(__name__, job_control.__name__, str(rm)))
            wait_queue.append(rm)


    logging.debug('{0} :: {1}  - FINISHING.'
    .format(__name__, job_control.__name__))


def process_metrics(p, rm):
    """ Worker process for requests -
        this will typically operate in a forked process """

    logging.info(__name__ + ' :: START JOB %s (PID = %s)' % (str(rm),
                                                             getpid()))

    # obtain user list - handle the case where a lone user ID is passed
    if search(MW_UID_REGEX, str(rm.cohort_expr)):
        users = [rm.cohort_expr]
    # Special case where user lists are to be generated based on registered
    # user reg dates from the logging table -- see src/metrics/users.py
    elif rm.cohort_expr == 'all':
        users = MediaWikiUser(query_type=1)
    else:
        users = get_users(rm.cohort_expr)

    # unpack RequestMeta into dict using MEDIATOR
    args = {attr.metric_var: getattr(rm, attr.query_var)
            for attr in QUERY_PARAMS_BY_METRIC[rm.metric]}
    logging.info(__name__ + ' :: Calling %s with args = %s.' % (rm.metric,
                                                                str(args)))

    # process request
    results = process_data_request(rm.metric, users, **args)

    p.put(jsonify(results))
    logging.info(__name__ + ' :: END JOB %s (PID = %s)' % (str(rm), getpid()))
