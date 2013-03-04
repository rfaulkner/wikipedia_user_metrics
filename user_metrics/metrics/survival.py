
__author__ = "Ryan Faulkner"
__date__ = "December 6th, 2012"
__license__ = "GPL (version 2 or later)"

import user_metric as um
import threshold as th
from user_metrics.etl.aggregator import decorator_builder, boolean_rate

class Survival(um.UserMetric):
    """
        Boolean measure of the retention of editors. Editors are considered "surviving" if they continue to participate
        after t minutes since an event (often, the user's registration or first edit).

            `https://meta.wikimedia.org/wiki/Research:Metrics/survival(t)`

        As a UserMetric type this class utilizes the process() function attribute to produce an internal list of metrics by
        user handle (typically ID but user names may also be specified). The execution of process() produces a nested list that
        stores in each element:

            * User ID
            * boolean flag to indicate whether the user met the survival criteria

        usage e.g.: ::

            >>> import user_metrics.etl.threshold as t
            >>> for r in t.Threshold().process([13234584]).__iter__(): print r
            (13234584L, 1)

    """

    # Structure that defines parameters for Survival class
    _param_types = {
        'init' : {
            't' : [int, 'The time in minutes registration '
                          'after which survival is measured.', 24],
            },
        'process' : {}
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields' : [0],
        'date_fields' : [],
        'float_fields' : [],
        'integer_fields' : [],
        'boolean_fields' : [1],
        }

    _agg_indices = {
        'list_sum_indices' : _data_model_meta['boolean_fields'],
        }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        super(Survival, self).__init__(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'is_alive']

    @um.UserMetric.pre_process_metric_call
    def process(self, user_handle, **kwargs):

        """
            Wraps the functionality of UserMetric::Threshold by setting the `survival` flag in process().

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).  Value or list of values representing user handle(s).
        """

        # Utilize threshold, survival is denoted by making at least one revision
        kwargs['survival_'] = True
        kwargs['n'] = 1

        self._results =  th.Threshold(**kwargs).process(user_handle, **kwargs)._results
        return self


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

# Build "rate" decorator
survival_editors_agg = boolean_rate
survival_editors_agg = decorator_builder(Survival.header())(survival_editors_agg)

setattr(survival_editors_agg, um.METRIC_AGG_METHOD_FLAG, True)
setattr(survival_editors_agg, um.METRIC_AGG_METHOD_NAME, 'survival_editors_agg')
setattr(survival_editors_agg, um.METRIC_AGG_METHOD_HEAD, ['total_users',
                                                           'has_survived','rate'])
setattr(survival_editors_agg, um.METRIC_AGG_METHOD_KWARGS, {'val_idx' : 1})

# testing
if __name__ == "__main__":
    # did these users survive after a day?
    for r in Survival().process([13234584, 156171], num_threads=2, log_progress=True).__iter__(): print r