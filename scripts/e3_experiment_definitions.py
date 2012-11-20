
"""
    Used to store data definitions for E3 experiments and related tables and log files.

    As new experiments are initiated the data definition over different storage mediums can be matained using
    the `experiemnt`
"""

with open('e3_experiment_definitions.py','r') as f:
    __doc__ =  " ".join(f.read().split("with open(")[0].strip().split('\n'))

__author__ = "ryan faulkner"
__date__ = "18/11/2012"
__license__ = "GPL (version 2 or later)"

from sys import path
import e3_settings as s
path.append(s.__E3_Analysis_Home__)

import src.etl.log_parser as lp
import src.etl.data_loader as dl
import copy

DEFINITION = "{<experiment_name> : { 'logfiles' : <list of files>, \n'start_date' : <start of experiment>, " \
             "\n'end_date' : <end of experiment>, \n'log_data' : { \n'server_logs' { \n'definition' : " \
             "<SQL table creation>, \n'table_name' : <table_name>, \n'log_parser_method' : " \
             "<function ref to parser method> }, \n'client_logs' : {<same def as 'server logs'>}, \n'<metric_1>' : " \
             "\n'definition' : <SQL table creation>, \n'table_name' : <table_name>, \n'log_parser_method' : " \
             "<function ref to parser method> }, ... <additional metrics> ..."
__doc__ = DEFINITION

experiments = {

    'cta4' : {
                'version' : 2,
                'log_files' : ['clicktracking.log-20121026.gz', 'clicktracking.log-20121027.gz',
                               'clicktracking.log-20121028.gz', 'clicktracking.log-20121029.gz',
                               'clicktracking.log-20121030.gz', 'clicktracking.log-20121031.gz',
                               'clicktracking.log-20121101.gz', 'clicktracking.log-20121102.gz',
                               'clicktracking.log-20121103.gz', 'clicktracking.log-20121104.gz',
                               'clicktracking.log-20121105.gz', 'clicktracking.log-20121106.gz',
                               'clicktracking.log-20121107.gz'],

                'start_date' : '20121026000000',
                'end_date' : '20121107000000',

                'log_data' : {

                    'server_logs' : { 'definition' : """
                                                    create table `e3_%s_server_logs` (
                                                    `project` varbinary(255) NOT NULL DEFAULT '',
                                                    `username` varbinary(255) NOT NULL DEFAULT '',
                                                    `user_id` varbinary(255) NOT NULL DEFAULT '',
                                                    `timestamp` varbinary(255) NOT NULL DEFAULT '',
                                                    `event_id` varbinary(255) NOT NULL DEFAULT '',
                                                    `self_made` varbinary(255) NOT NULL DEFAULT '',
                                                    `version` varbinary(255) NOT NULL DEFAULT '',
                                                    `by_email` varbinary(255) NOT NULL DEFAULT '',
                                                    `creator_user_id` varbinary(255) NOT NULL DEFAULT ''
                                                    ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                """,
                                  'table_name' : 'e3_%s_server_logs',
                                  'log_parser_method' : lp.LineParseMethods.e3_cta4_log_parse_server
                    },

                    'client_logs' : { 'definition' : """
                                                        create table `e3_%s_client_logs` (
                                                        `project` varbinary(255) NOT NULL DEFAULT '',
                                                        `event_signature` varbinary(255) NOT NULL DEFAULT '',
                                                        `event_type` varbinary(255) NOT NULL DEFAULT '',
                                                        `timestamp` varbinary(255) NOT NULL DEFAULT '',
                                                        `token` varbinary(255) NOT NULL DEFAULT '',
                                                        `add_field_1` varbinary(255) NOT NULL DEFAULT '',
                                                        `add_field_2` varbinary(255) NOT NULL DEFAULT '',
                                                        `add_field_3` varbinary(255) NOT NULL DEFAULT ''
                                                        ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                                        'table_name' : 'e3_%s_client_logs',
                                        'log_parser_method' : lp.LineParseMethods.e3_cta4_log_parse_client

                    }
                },

                'metric_tables' : {
                        'blocks' : { 'definition' : """
                                                create table `e3_%s_blocks` (
                                                `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                  `block_count` varbinary(255) NOT NULL DEFAULT '',
                                                  `first_block` varbinary(255) NOT NULL DEFAULT '',
                                                  `last_block` varbinary(255) NOT NULL DEFAULT '',
                                                  `ban` varbinary(255) NOT NULL DEFAULT ''
                                                ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                                      'table_name' : 'e3_%s_blocks'
                    },

                    'edit_volume' : { 'definition' : """
                                                    CREATE TABLE `e3_%s_edit_volume` (
                                                  `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                  `bytes_added_net` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_abs` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_pos` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_neg` varbinary(255) NOT NULL DEFAULT '',
                                                  `edit_count` varbinary(255) NOT NULL DEFAULT ''
                                                ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                                      'table_name' : 'e3_%s_edit_volume'
                    },

                    'time_to_milestone' : { 'definition' : """
                                                            CREATE TABLE `e3_%s_time_to_milestone` (
                                                              `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                              `time_minutes` varbinary(255) NOT NULL DEFAULT ''
                                                            ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                                      'table_name' : 'e3_%s_time_to_milestone'
                    }
                },

                'user_list_sql' : "select distinct user_id from e3_cta4_server_logs"


    },

    'acux2' : {
        'version' : 4,
        'log_files' : ['clicktracking.log-20121018.gz', 'clicktracking.log-20121019.gz',
                       'clicktracking.log-20121020.gz', 'clicktracking.log-20121021.gz',
                       'clicktracking.log-20121022.gz', 'clicktracking.log-20121023.gz',
                       'clicktracking.log-20121024.gz', 'clicktracking.log-20121025.gz',
                       'clicktracking.log-20121026.gz', 'clicktracking.log-20121027.gz',
                       'clicktracking.log-20121028.gz', 'clicktracking.log-20121029.gz',
                       'clicktracking.log-20121030.gz', 'clicktracking.log-20121031.gz',
                       'clicktracking.log-20121101.gz', 'clicktracking.log-20121102.gz',
                       'clicktracking.log-20121103.gz', 'clicktracking.log-20121104.gz',
                       'clicktracking.log-20121105.gz', 'clicktracking.log-20121106.gz',
                       'clicktracking.log-20121107.gz'],

        'start_date' : '20121026000000',
        'end_date' : '20121107000000',

        'log_data' : {
            'server_logs' : { 'definition' : """
                                                        create table `e3_%s_server_events` (
                                                        `project` varbinary(255) NOT NULL DEFAULT '',
                                                        `username` varbinary(255) NOT NULL DEFAULT '',
                                                        `user_id` varbinary(255) NOT NULL DEFAULT '',
                                                        `timestamp` varbinary(255) NOT NULL DEFAULT '',
                                                        `event_id` varbinary(255) NOT NULL DEFAULT '',
                                                        `self_made` varbinary(255) NOT NULL DEFAULT '',
                                                        `mw_user_token` varbinary(255) NOT NULL DEFAULT '',
                                                        `version` varbinary(255) NOT NULL DEFAULT '',
                                                        `by_email` varbinary(255) NOT NULL DEFAULT '',
                                                        `creator_user_id` varbinary(255) NOT NULL DEFAULT ''
                                                        ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                              'table_name' : 'e3_%s_server_events',
                              'log_parser_method' : lp.LineParseMethods.e3_acux_log_parse_server_event
            },

            'client_logs' : { 'definition' : """
                                                    create table `e3_%s_client_events` (
                                                    `project` varbinary(255) NOT NULL DEFAULT '',
                                                    `bucket` varbinary(255) NOT NULL DEFAULT '',
                                                    `event` varbinary(255) NOT NULL DEFAULT '',
                                                    `timestamp` varbinary(255) NOT NULL DEFAULT '',
                                                    `user_category` varbinary(255) NOT NULL DEFAULT '',
                                                    `token` varbinary(255) NOT NULL DEFAULT '',
                                                    `namespace` varbinary(255) NOT NULL DEFAULT '',
                                                    `add_field_1` varbinary(255) NOT NULL DEFAULT '',
                                                    `add_field_2` varbinary(2000) NOT NULL DEFAULT '',
                                                    `add_field_3` varbinary(255) NOT NULL DEFAULT ''
                                                    ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                """,
                              'table_name' : 'e3_%s_client_events',
                              'log_parser_method' : lp.LineParseMethods.e3_acux_log_parse_client_event
                }
        },

        'user_list' : lambda d: [d.__setitem__(row[0],row[1]) for row in dl.Connector(
            instance = 'slave').execute_SQL('select user_id, origin '
                                              'from e3_acux_cta_deduped_users where origin != "aftv5_cta4"')],

        'metric_tables' : {
            'blocks' : { 'definition' : """
                                                create table `e3_%s_blocks` (
                                                `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                  `bucket` varbinary(255) NOT NULL DEFAULT '',
                                                  `block_count` varbinary(255) NOT NULL DEFAULT '',
                                                  `first_block` varbinary(255) NOT NULL DEFAULT '',
                                                  `last_block` varbinary(255) NOT NULL DEFAULT '',
                                                  `ban` varbinary(255) NOT NULL DEFAULT ''
                                                ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                         'table_name' : 'e3_%s_blocks'
            },

            'edit_volume' : { 'definition' : """
                                                    CREATE TABLE `e3_%s_edit_volume` (
                                                  `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                  `bucket` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_net` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_abs` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_pos` varbinary(255) NOT NULL DEFAULT '',
                                                  `bytes_added_neg` varbinary(255) NOT NULL DEFAULT '',
                                                  `edit_count` varbinary(255) NOT NULL DEFAULT ''
                                                ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                              'table_name' : 'e3_%s_edit_volume'
            },

            'time_to_milestone' : { 'definition' : """
                                                            CREATE TABLE `e3_%s_time_to_milestone` (
                                                              `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                              `bucket` varbinary(255) NOT NULL DEFAULT '',
                                                              `time_minutes` varbinary(255) NOT NULL DEFAULT ''
                                                            ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                    """,
                                    'table_name' : 'e3_%s_time_to_milestone'
            }
        }
    }
}

experiments['acux3'] = copy.deepcopy(experiments['acux2'])
experiments['acux3']['version'] = 3
experiments['acux3']['log_files'] = ['clicktracking.log-20121106.gz', 'clicktracking.log-20121107.gz',
                                      'clicktracking.log-20121108.gz', 'clicktracking.log-20121109.gz',
                                      'clicktracking.log-20121110.gz', 'clicktracking.log-20121111.gz',
                                      'clicktracking.log-20121112.gz', 'clicktracking.log-20121113.gz',
                                      'clicktracking.log-20121114.gz', 'clicktracking.log-20121115.gz',
                                      'clicktracking.log-20121116.gz']

experiments['acux3']['start_date'] = '2012110600000'
experiments['acux3']['start_date'] = '2012112000000'

# Add experiment name to tables
for exp in experiments.keys():

    data_tables = experiments[exp]['log_data']
    metric_tables = experiments[exp]['metric_tables']

    for key in data_tables:
        data_tables[key]['table_name'] = data_tables[key]['table_name'] % exp
        data_tables[key]['definition'] = data_tables[key]['definition'] % exp
    for key in metric_tables:
        metric_tables[key]['table_name'] = metric_tables[key]['table_name'] % exp
        metric_tables[key]['definition'] = metric_tables[key]['definition'] % exp