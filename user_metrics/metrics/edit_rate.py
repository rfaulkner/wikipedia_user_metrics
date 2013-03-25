
__author__ = "Ryan Faulkner"
__date__ = "July 27th, 2012"
__license__ = "GPL (version 2 or later)"

from copy import deepcopy
from dateutil.parser import parse as date_parse
import user_metric as um
import edit_count as ec
from user_metrics.etl.aggregator import weighted_rate, decorator_builder, \
    build_numpy_op_agg, build_agg_meta
from numpy import median, min, max, mean, std
from user_metrics.metrics.users import USER_METRIC_PERIOD_TYPE as umpt
from user_metrics.utils import enum, format_mediawiki_timestamp
from user_metrics.metrics.user_metric import METRIC_AGG_METHOD_KWARGS


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
    TIME_UNIT_TYPE = enum('HOUR', 'DAY')

    # Structure that defines parameters for EditRate class
    _param_types = {
        'init': {}
    }
    _param_types = {
        'init': {
            'time_unit': [int, 'Type of time unit to normalize by '
                               '(HOUR=0, DAY=1).', TIME_UNIT_TYPE.HOUR],
            'time_unit_count': [int,
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
        super(EditRate, self).__init__(**kwargs)

    @staticmethod
    def header():
        return ['user_id', 'edit_count', 'edit_rate', 'period_len']

    @um.UserMetric.pre_process_metric_call
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

        # Extract edit count for given parameters
        edit_rate = list()
        ec_kwargs = deepcopy(self.__dict__)
        e = ec.EditCount(**ec_kwargs).process(user_handle, **kwargs)

        # Compute time difference between datetime objects and get the
        # integer number of seconds

        if self.group == umpt.REGISTRATION:
            time_diff_sec = self.t * 3600.0
        elif self.group == umpt.INPUT:
            try:
                start_ts_obj = date_parse(
                    format_mediawiki_timestamp(self.datetime_start))
                end_ts_obj = date_parse(
                    format_mediawiki_timestamp(self.datetime_end))
            except (AttributeError, ValueError):
                raise um.UserMetricError()

            time_diff_sec = (end_ts_obj - start_ts_obj).total_seconds()
        else:
            raise um.UserMetricError('group parameter not specified.')

        # Normalize the time interval based on the measure
        if self.time_unit == self.TIME_UNIT_TYPE.DAY:
            time_diff = time_diff_sec / (24 * 60 * 60)
        elif self.time_unit == self.TIME_UNIT_TYPE.HOUR:
            time_diff = time_diff_sec / (60 * 60)
        else:
            time_diff = time_diff_sec

        # Build the list of edit rate metrics
        for i in e.__iter__():
            new_i = i[:]  # Make a copy of the edit count element
            new_i.append(new_i[1] / (time_diff * self.time_unit_count))
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
})

metric_header = EditRate.header()

field_prefixes = \
    {
        'count_': 1,
        'rate_': 2,
    }


# Build "dist" decorator
op_list = [sum, mean, std, median, min, max]
er_stats_agg = build_numpy_op_agg(build_agg_meta(op_list, field_prefixes),
                                  metric_header,
                                  'er_stats_agg')

agg_kwargs = getattr(er_stats_agg, METRIC_AGG_METHOD_KWARGS)
setattr(er_stats_agg, METRIC_AGG_METHOD_KWARGS, agg_kwargs)
