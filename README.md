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

`wmf_user_metrics` is packaged with distutils: ::

    $ sudo pip install wmf_user_metrics

Once installed you will need to modify the configuration files.  This
can be found in the file `settings.py` under
`$site-packages-home$/e3_analysis/config`.  Within this file configure
the connections dictionary to point to a replicated production MySQL instance
containing the .  The 'db' setting should be an instance which 'user' has write
access to.  If you are from outside the Wikimedia Foundation and do not have
access to these credentials contact me at rfaulkner@wikimedi.org if you'd
like to work with this package.

The template configuration file looks like the following: ::

    # Project settings
    # ================
    __home__ = '/Users/rfaulkner/'
    __project_home__ = ''.join([__home__, 'projects/E3_analysis/'])
    __web_home__ = ''.join([__project_home__, 'web_interface/'])
    __sql_home__ = ''.join([__project_home__, 'SQL/'])
    __server_log_local_home__ = ''.join([__project_home__, 'logs/'])
    __data_file_dir__ = ''.join([__project_home__, 'data/'])

    __web_app_module__ = 'web_interface'
    __system_user__ = 'rfaulk'

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

Documentation
-------------

Once the installation is complete and the configuration has been set the
modules can be imported into the Python environment.  The available
operational modules are the following: ::

    src.etl.data_loader
    src.etl.aggregator
    src.etl.table_loader
    src.etl.log_parser
    src.etl.time_series_process_methods
    src.etl.wpapi

    src.metrics.blocks
    src.metrics.bytes_added
    src.metrics.live_account.pyc
    src.metrics.edit_count
    src.metrics.edit_rate
    src.metrics.live_account
    src.metrics.metrics_manager
    src.metrics.namespace_of_edits
    src.metrics.query_calls
    src.metrics.revert_rate
    src.metrics.survival
    src.metrics.time_to_threshold
    src.metrics.user_metric
    src.metrics.users

    src.utils.autovivification
    src.utils.multiprocessing_wrapper
    src.utils.record_type

More complete docs can be found at:
    http://stat1.wikimedia.org/rfaulk/pydocs/_build/
