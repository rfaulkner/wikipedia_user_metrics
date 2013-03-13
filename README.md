wmf_user_metrics
================


Introduction
------------

This package implements log retrieval, metrics generation, and data
analysis tools used by the Editor Engagement Experiment (E3) team at
the Wikimedia Foundation. The modules herein will be used to perform
the ETL and analysis operations necessary to process the experimental
data generated from E3 projects.

Installation
------------

`wmf_user_metrics` is packaged with distutils:

    $ sudo pip install wmf_user_metrics

Once installed you will need to modify the configuration files.  This
can be found in the file `settings.py` under
`$site-packages-home$/e3_analysis/config`.  Within this file configure
the connections dictionary to point to a replicated production MySQL instance
containing the .  The 'db' setting should be an instance which 'user' has write
access to.  If you are from outside the Wikimedia Foundation and do not have
access to these credentials contact me at rfaulkner@wikimedi.org if you'd
like to work with this package.

The template configuration file looks like the following:

    # Project settings
    # ================

    __project_home__ = realpath('../..') + '/'
    __web_home__ = ''.join([__project_home__, 'src/api/'])
    __data_file_dir__ = ''.join([__project_home__, 'data/'])

    __query_module__ = 'query_calls_noop'
    __user_thread_max__ = 100
    __rev_thread_max__ = 50

    # Database connection settings
    # ============================

    connections = {
        'slave': {
            'user' : 'research',
            'host' : '127.0.0.1',
            'db' : 'staging',
            'passwd' : 'xxxx',
            'port' : 3307},
        'slave-2': {
            'user' : 'rfaulk',
            'host' : '127.0.0.1',
            'db' : 'rfaulk',
            'passwd' : 'xxxx',
            'port' : 3307}
    }

    PROJECT_DB_MAP = {
        'enwiki': 's1',
        'dewiki': 's5',
        'itwiki': 's2',
    }

    # SSH Tunnel Parameters
    # =====================

    TUNNEL_DATA = {
        's1': {
            'cluster_host': 'stats',
            'db_host': 's1-db',
            'user': 'xxxx',
            'remote_port': 3306,
            'tunnel_port': 3307
        },
        's2': {
            'cluster_host': 'stats',
            'db_host': 's2-db',
            'user': 'xxxx',
            'remote_port': 3306,
            'tunnel_port': 3308
        }
    }

Documentation
-------------

Once the installation is complete and the configuration has been set the
modules can be imported into the Python environment.  The available
operational modules are the following:

    user_metrics.api
    user_metrics.api.run
    user_metrics.api.views

    user_metrics.api.engine
    user_metrics.api.engine.data
    user_metrics.api.engine.request_manager
    user_metrics.api.engine.request_meta

    user_metrics.etl
    user_metrics.etl.data_loader
    user_metrics.etl.aggregator
    user_metrics.etl.table_loader
    user_metrics.etl.log_parser
    user_metrics.etl.time_series_process_methods
    user_metrics.etl.wpapi

    user_metrics.metrics
    user_metrics.metrics.blocks
    user_metrics.metrics.bytes_added
    user_metrics.metrics.live_account.pyc
    user_metrics.metrics.edit_count
    user_metrics.metrics.edit_rate
    user_metrics.metrics.live_account
    user_metrics.metrics.metrics_manager
    user_metrics.metrics.namespace_of_edits
    user_metrics.metrics.query_calls
    user_metrics.metrics.revert_rate
    user_metrics.metrics.survival
    user_metrics.metrics.time_to_threshold
    user_metrics.metrics.user_metric
    user_metrics.metrics.users

    user_metrics.query
    user_metrics.query.query_calls_noop
    user_metrics.query.query_calls_sql

    user_metrics.utils
    user_metrics.utils.autovivification
    user_metrics.utils.multiprocessing_wrapper
    user_metrics.utils.record_type

More complete docs can be found at:
    http://stat1.wikimedia.org/rfaulk/pydocs/_build/
