
from sys import path
import settings as s
path.append(s.__E3_Analysis_Home__)

import src.etl.log_parser as lp

experiments = {

    'CTA4' : {
                'log_files' : ['clicktracking.log-20121026.gz',
                               'clicktracking.log-20121028.gz', 'clicktracking.log-20121029.gz',
                               'clicktracking.log-20121030.gz', 'clicktracking.log-20121031.gz',
                               'clicktracking.log-20121101.gz', 'clicktracking.log-20121102.gz',
                               'clicktracking.log-20121103.gz', 'clicktracking.log-20121104.gz',
                               'clicktracking.log-20121105.gz', 'clicktracking.log-20121106.gz',
                               'clicktracking.log-20121107.gz'],

                'start_date' : '20121026000000',
                'end_date' : '20121107000000',
                'log_parser_method' : lp.LineParseMethods.e3_cta4_log_parse,

                'user_bucket' : { 'definition' : """
                                                    create table `e3_cta4_users` (
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
                                    'table_name' : 'e3_cta4_users'
                                },

                'blocks' : { 'definition' : """
                                            create table `e3_cta4_blocks` (
                                            `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                              `block_count` varbinary(255) NOT NULL DEFAULT '',
                                              `first_block` varbinary(255) NOT NULL DEFAULT '',
                                              `last_block` varbinary(255) NOT NULL DEFAULT '',
                                              `ban` varbinary(255) NOT NULL DEFAULT ''
                                            ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                """,
                                  'table_name' : 'e3_cta4_blocks'
                },

                'edit_volume' : { 'definition' : """
                                                CREATE TABLE `e3_cta4_edit_volume` (
                                              `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                              `bytes_added_net` varbinary(255) NOT NULL DEFAULT '',
                                              `bytes_added_abs` varbinary(255) NOT NULL DEFAULT '',
                                              `bytes_added_pos` varbinary(255) NOT NULL DEFAULT '',
                                              `bytes_added_neg` varbinary(255) NOT NULL DEFAULT '',
                                              `edit_count` varbinary(255) NOT NULL DEFAULT ''
                                            ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                """,
                                  'table_name' : 'e3_cta4_edit_volume'
                },

                'time_to_milestone' : { 'definition' : """
                                                        CREATE TABLE `e3_cta4_time_to_milestone` (
                                                          `user_id` int(5) unsigned NOT NULL DEFAULT 0,
                                                          `time_minutes` varbinary(255) NOT NULL DEFAULT ''
                                                        ) ENGINE=MyISAM DEFAULT CHARSET=binary
                                                """,
                                  'table_name' : 'e3_cta4_time_to_milestone'
                },

                'user_list_sql' : "select distinct rev_user from e3_cta4_users as e "
                                  "join enwiki.revision as r on e.add_field_3 = r.rev_id and rev_user > 0"

    }
}