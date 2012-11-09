
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

                'bytes_added' : { 'definition' : '',
                                  'table_name' : ''
                }
    }
}