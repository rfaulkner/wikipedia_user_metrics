
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from os import getpid
from collections import namedtuple
import user_metric as um
from user_metrics.metrics import query_mod
from user_metrics.metrics.users import UMP_MAP
from user_metrics.utils import multiprocessing_wrapper as mpw
from user_metrics.config import logging


class EditCount(um.UserMetric):
    """
        Produces a count of edits as well as the total number of bytes added
        for a registered user.

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_count(t)`

            usage e.g.: ::

                >>> import classes.Metrics as m
                >>> m.EditCount(date_start='2012-12-12 00:00:00',date_end=
                    '2012-12-12 00:00:00',namespace=0).process(123456)
                25, 10000

            The output in this case is the number of edits (25) made by the
            editor with ID 123456 and the total number of bytes added by those
            edits (10000)
    """

    # Structure that defines parameters for EditRate class
    _param_types = {
        'init': {},
        'process': {
            'k': [int, 'Number of worker processes.', 5]
        }
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields': [0],
        'date_fields': [],
        'float_fields': [],
        'integer_fields': [1],
        'boolean_fields': [],
    }

    _agg_indices = {
        'list_sum_indices': _data_model_meta['integer_fields'] +
        _data_model_meta['float_fields'],
    }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(EditCount, self).__init__(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'edit_count']

    @um.UserMetric.pre_process_metric_call
    def process(self, users, **kwargs):
        """
            Determine edit count.  The parameter *user_handle* can be either
            a string or an integer or a list of these types.  When the
            *user_handle* type is integer it is interpreted as a user id, and
            as a user_name for string input.  If a list of users is passed
            to the *process* method then a dict object with edit counts keyed
            by user handles is returned.

            - Paramters:
                - **user_handle** - String or Integer (optionally lists):
                    Value or list of values representing user handle(s).
                - **is_id** - Boolean.  Flag indicating whether user_handle
                    stores user names or user ids
        """

        # Pack args, call thread pool
        args = self._pack_params()
        results = mpw.build_thread_pool(users, _process_help,
                                        self.k_, args)

        # Get edit counts from query - all users not appearing have
        # an edit count of 0
        user_set = set([long(user_id) for user_id in users])
        edit_count = list()
        for row in results:
            edit_count.append([row[0], int(row[1])])
            user_set.discard(row[0])
        for user in user_set:
            edit_count.append([user, 0])

        self._results = edit_count
        return self


def _process_help(args):
    """
        Worker thread method for edit count.
    """

    # Unpack args
    users = args[0]
    state = args[1]

    metric_params = um.UserMetric._unpack_params(state)
    query_args_type = namedtuple('QueryArgs', 'date_start date_end')

    logging.debug(__name__ + ':: Executing EditCount on '
                             '%s users (PID = %s)' % (len(users), getpid()))

    # Call user period method
    umpd_obj = UMP_MAP[metric_params.group](users, metric_params)
    results = list()
    for t in umpd_obj:
        args = query_args_type(t.start, t.end)

        # Build edit count results list
        results += query_mod.edit_count_user_query(t.user,
                                                   metric_params.project,
                                                   args)
    return results


# Rudimentary Testing
if __name__ == '__main__':
    users = ['13234584', '13234503', '13234565', '13234585', '13234556']
    e = EditCount(t=10000)

    # Check edit counts against
    for res in e.process(users):
        print res
