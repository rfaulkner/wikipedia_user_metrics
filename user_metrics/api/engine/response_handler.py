"""
    Handles API responses.
"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2013-03-14"
__license__ = "GPL (version 2 or later)"

from collections import OrderedDict
from user_metrics.config import logging
from user_metrics.api.engine.request_meta import rebuild_unpacked_request
from user_metrics.api.engine.data import set_data, build_key_signature
from Queue import Empty

# Timeout in seconds to wait for data on the queue.  This should be long
# enough to ensure that the full response can be received
RESPONSE_TIMEOUT = 0.1


# API RESPONSE HANDLER
# ####################


def process_responses(response_queue, msg_in):
    """ Pulls responses off of the queue. """

    logging.debug('{0} :: {1}  - STARTING...'
        .format(__name__, process_responses.__name__))

    while 1:
        stream = ''

        # Block on the response queue
        try:
            res = response_queue.get(True)
            request_meta = rebuild_unpacked_request(res)
        except Exception:
            logging.error(__name__ + ' :: Could not get request meta')
            continue

        data = response_queue.get(True)
        while data:
            stream += data
            try:
                data = response_queue.get(True, timeout=1)
            except Empty:
                break

        try:
            data = eval(stream)
        except Exception:
            logging.error(__name__ + ' :: Request {0} failed.'.
                format(request_meta))
            continue

        key_sig = build_key_signature(request_meta, hash_result=True)

        # Set request in list to "not alive"
        msg_in.put([1, key_sig], True)

        logging.debug(__name__ + ' :: Setting data for {0}'.
            format(str(request_meta)))
        set_data(stream, request_meta)

    logging.debug('{0} :: {1}  - SHUTTING DOWN...'
        .format(__name__, process_responses.__name__))