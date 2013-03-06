"""
    This module implements the request manager functionality.

    Job Queue and Processing
    ^^^^^^^^^^^^^^^^^^^^^^^^

    As requests are issued via http to the API a process queue will store all
    active jobs. Processes will be created and assume one of the following
    states throughout their existence: ::

        * 'pending' - The request has yet to be begin being processed
        * 'running' - The request is being processed
        * 'success' - The request has finished processing and is exposed at
            the url
        * 'failure' - The result has finished processing but dailed to expose
            results

    When a process a request is received and a job is created to service that
    request it enters the 'pending' state. If the job returns without
    exception it enters the 'success' state, otherwise it enters the 'failure'
    state.  The job remains in either of these states until it is cleared
    from the process queue.

    Response Data
    ^^^^^^^^^^^^^

    As requests are made to the API the data generated and formatted as JSON.
    The definition of is as follows: ::

        {   header : header_list,
            cohort_expr : cohort_gen_timestamp : metric : timeseries :
            aggregator : date_start : date_end : [ metric_param : ]* : data
        }

    Where each component is defined: ::

        header_str := list(str), list of header values
        cohort_expr := str, cohort ID expression
        cohort_gen_timestamp := str, cohort generation timestamp (earliest of
            all cohorts in expression)
        metric := str, user metric handle
        timeseries := boolean, indicates if this is a timeseries
        aggregator := str, aggregator used
        date_start := str, start datetime of request
        date_end := str, end datetime of request
        metric_param := -, optional metric parameters
        data := list(tuple), set of data points

    Request data is mapped to a query via metric objects and hashed in the
    dictionary `api_data`.

"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2013-03-05"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging
from user_metrics.api import api_data
from user_metrics.api.engine import MAX_CONCURRENT_JOBS, \
    QUEUE_WAIT, MW_UID_REGEX
from user_metrics.api.engine.data import get_users, set_data
from user_metrics.api.engine.request_meta import QUERY_PARAMS_BY_METRIC, \
    RequestMetaFactory
from user_metrics.metrics.users import MediaWikiUser
from user_metrics.metrics.metrics_manager import process_data_request

from multiprocessing import Process, Queue
from collections import namedtuple
from re import search
from os import getpid

job_item_type = namedtuple('JobItem', 'id process request queue')


def job_control(request_queue):
    """
        Controls the execution of user metrics requests

        Parameters
        ~~~~~~~~~~

        request_queue : multiprocessing.Queue
           Queues incoming API requests.

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

            logging.debug('{0} :: {1}' \
                          '\n\tPull item from request queue -> \n\t{2}'
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
                data = job_item.queue.get()
                set_data(job_item.request, data, api_data)

                del job_queue[job_queue.index(job_item)]

                concurrent_jobs -= 1

                logging.debug('{0} :: {1}\n\tRUN -> RESPONSE {2}\n' \
                              '\tConcurrent jobs = {3}'
                .format(__name__, job_control.__name__, str(job_item),
                        concurrent_jobs))


        # Process pending jobs
        # --------------------

        for wait_req in wait_queue:
            if concurrent_jobs <= MAX_CONCURRENT_JOBS:
                # prepare job from item

                req_q = Queue()
                proc = Process(target=process_metrics, args=(req_q, wait_req))
                proc.start()

                job_item = job_item_type(job_id, proc, wait_req, req_q)
                job_queue.append(job_item)

                del wait_queue[wait_queue.index(wait_req)]

                concurrent_jobs += 1
                job_id += 1

                logging.debug('{0} :: {1}\n\tWAIT -> RUN {2}\n' \
                              '\tConcurrent jobs = {3}'\
                .format(__name__, job_control.__name__, str(wait_req),
                        concurrent_jobs))


        # Add newest job to the queue
        # ---------------------------

        if req_item and concurrent_jobs <= MAX_CONCURRENT_JOBS:

            # Build the request item
            rm = RequestMetaFactory(req_item['cohort_expr'],
                                    req_item['cohort_gen_timestamp'],
                                    req_item['metric'])

            # Populate the request data
            for key in req_item:
                if req_item[key]:
                    setattr(rm, key, req_item[key])

            logging.debug('{0} :: {1}\n\tREQUEST -> WAIT {2}'
            .format(__name__, job_control.__name__, str(req_item)))
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
    p.put(results)

    logging.info(__name__ + ' :: END JOB %s (PID = %s)' % (str(rm), getpid()))

