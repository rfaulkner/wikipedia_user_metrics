
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from dateutil.parser import parse as date_parse
import user_metric as um
import edit_count as ec
from user_metrics.etl.aggregator import weighted_rate, decorator_builder, \
    build_numpy_op_agg, build_agg_meta
from numpy import median, min, max


class EditRate(um.UserMetric):
    """
        Produces a float value that reflects the rate of edit behaviour

            `https://meta.wikimedia.org/wiki/Research:Metrics/edit_rate`

        As a UserMetric type this class utilizes the process() function
        attribute to produce an internal list of metrics by user handle
        (typically ID but user names may also be specified). The execution
        of process() produces a nested list that stores in each element:

            * User ID
            * edit count
            * edit rate
            * start time
            * period length

            usage e.g.: ::

                >>> import classes.Metrics as m
                >>> m.EditRate(date_start='2012-12-12 00:00:00',
                    date_end='2012-12-12 00:00:00', namespace=3).
                        process(123456)
                2.50
    """

    # Constants for denoting the time unit by which to
    # normalize edit counts to produce an edit rate
    HOUR = 0
    DAY = 1

    # Structure that defines parameters for EditRate class
    _param_types = {
        'init': {}
    }
    _param_types = {
        'init': {
            'time_unit': ['int', 'Type of time unit to normalize by '
                                 '(HOUR=0, DAY=1).', DAY],
            'time_unit_count': ['int',
                                'Number of time units to normalize '
                                'by (e.g. per two days).', 1],
        },
        'process': {}
    }

    # Define the metrics data model meta
    _data_model_meta = {
        'id_fields': [0],
        'date_fields': [3],
        'float_fields': [2],
        'integer_fields': [1, 4],
        'boolean_fields': [],
    }

    _agg_indices = {
        'list_sum_indices': _data_model_meta['integer_fields'] +
        _data_model_meta['float_fields'],
    }

    @um.pre_metrics_init
    def __init__(self, **kwargs):
        um.UserMetric.__init__(self, **kwargs)
        self._time_unit_count_ = kwargs['time_unit_count']
        self._time_unit_ = kwargs['time_unit']

    @staticmethod
    def header():
        return ['user_id', 'edit_count', 'edit_rate',
                'start_time', 'period_len']

    @um.UserMetric.pre_process_users
    def process(self, user_handle, **kwargs):
        """
            Determine the edit rate of user(s).  The parameter *user_handle*
            can be either a string or an integer or a list of these types.
            When the *user_handle* type is integer it is interpreted as a user
            id, and as a user_name for string input.  If a list of users is
            passed to the *process* method then a dict object with edit rates
            keyed by user handles is returned.

            - Parameters:
                - **user_handle** - String or Integer (optionally lists).
                    Value or list of values representing user handle(s).

            - Return:
                - Dictionary. key(string): user handle, value(Float):
                edit counts
        """
        self.apply_default_kwargs(kwargs, 'process')

        # Extract edit count for given parameters
        edit_rate = list()
        e = ec.EditCount(date_start=self._start_ts_,
                         date_end=self._end_ts_,
                         datasource=self._data_source_,
                         namespace=self._namespace_).process(user_handle)

        try:
            start_ts_obj = date_parse(self._start_ts_)
            end_ts_obj = date_parse(self._end_ts_)
        except AttributeError:
            raise um.UserMetric.UserMetricError()
        except ValueError:
            raise um.UserMetric.UserMetricError()

        # Compute time difference between datetime objects and get the
        # integer number of seconds
        time_diff_sec = (end_ts_obj - start_ts_obj).total_seconds()

        if self._time_unit_ == EditRate.DAY:
            time_diff = time_diff_sec / (24 * 60 * 60)
        elif self._time_unit_ == EditRate.HOUR:
            time_diff = time_diff_sec / (60 * 60)
        else:
            time_diff = time_diff_sec

        # Build the list of edit rate metrics
        for i in e.__iter__():
            new_i = i[:]  # Make a copy of the edit count element
            new_i.append(new_i[1] / (time_diff * self._time_unit_count_))
            new_i.append(self._start_ts_)
            new_i.append(time_diff)
            edit_rate.append(new_i)
        self._results = edit_rate
        return self


# ==========================
# DEFINE METRIC AGGREGATORS
# ==========================

# Build "rate" decorator
edit_rate_agg = weighted_rate
edit_rate_agg = decorator_builder(EditRate.header())(edit_rate_agg)

setattr(edit_rate_agg, um.METRIC_AGG_METHOD_FLAG, True)
setattr(edit_rate_agg, um.METRIC_AGG_METHOD_NAME, 'edit_rate_agg')
setattr(edit_rate_agg, um.METRIC_AGG_METHOD_HEAD, ['total_users',
                                                   'total_weight', 'rate'])
setattr(edit_rate_agg, um.METRIC_AGG_METHOD_KWARGS, {
    'val_idx': 2,
    'data_type': 'float16'
})

metric_header = EditRate.header()

field_prefixes = \
    {
        'count_': 1,
        'rate_': 2,
    }

# Build "dist" decorator
op_list = [median, min, max]
er_stats_agg = build_numpy_op_agg(build_agg_meta(op_list, field_prefixes),
                                  metric_header,
                                  'er_stats_agg')
