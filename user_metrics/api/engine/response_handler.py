"""
    Handles API responses.
"""

__author__ = {
    "ryan faulkner": "rfaulkner@wikimedia.org"
}
__date__ = "2013-03-14"
__license__ = "GPL (version 2 or later)"

from user_metrics.config import logging
from flask import jsonify, make_response
from user_metrics.api.engine.request_meta import rebuild_unpacked_request
from user_metrics.api.engine.data import set_data, build_key_signature


# API RESPONSE HANDLER
# ####################


def process_responses(response_queue, requests_made, cache_ref):
    """ Pulls responses off of the queue. """

    logging.debug('{0} :: {1}  - STARTING...'
    .format(__name__, process_responses.__name__))

    while 1:
        # Block on the response queue
        res = response_queue.get(True)

        request_meta = rebuild_unpacked_request(res[0])
        key_sig = build_key_signature(request_meta, hash_result=True)

        # Set request in list to "not alive"
        if key_sig in requests_made:
            requests_made[key_sig][0] = False

        data = make_response(jsonify(res[1]))
        set_data(cache_ref, data, request_meta)

    logging.debug('{0} :: {1}  - SHUTTING DOWN...'
    .format(__name__, process_responses.__name__))