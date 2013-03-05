"""
    This module implements the request manager functionality.
"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2013-03-05"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging
from user_metrics.api.engine import JOB_STATUS, MAX_CONCURRENT_JOBS, \
    QUEUE_WAIT
from user_metrics.utils import build_namedtuple, record_type


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
        try:
            # Pull an item off of the queue
            item = request_queue.get(timeout=QUEUE_WAIT)

            logging.debug('{0} :: {1}  - {2}'
            .format(__name__, job_control.__name__, str(item)))

        except Exception:
            item = None
            logging.debug('{0} :: {1}  - {2}'
            .format(__name__, job_control.__name__, 'Listening ...'))


        # Process complete jobs
        for item in job_queue:
            #
            if not item.proc.is_alive():
                response_queue.put(item)
                del job_queue[job_queue.index(item)]
                concurrent_jobs -= 1

        # Process pending jobs
        for item in wait_queue:
            if concurrent_jobs <= MAX_CONCURRENT_JOBS:
                # prepare job from item
                job_queue.append(item)


        # Add newest job to the queue
        if item and concurrent_jobs <= MAX_CONCURRENT_JOBS:
            wait_queue.append(item)


    logging.debug('{0} :: {1}  - FINISHING.'
    .format(__name__, job_control.__name__))