
e3_analysis Package Install & Setup
===================================

This package implements log retrieval, metrics generation, and data analysis tools used by the Editor Engagement Experiment (E3) team at the Wikimedia Foundation. The modules herein will be used to perform the ETL and analysis operations necessary to process the experimental data generated from E3 projects.

Installation
------------

`e3_analysis` is packaged with distutils: ::

    $ sudo pip install e3_analysis 

Once installed you will need to modify the configuration files.  This can be found in the file `settings.py` under `$site-packages-home$/e3_analysis/config`.  Within this file configure the connections dictionary to point to a replicated production MySQL instance containing the .  The 'db' setting should be an instance which 'user' has write access to.  If you are from outside the Wikimedia Foundation and do not have access to these credentials contact me at rfaulkner@wikimedi.org if you'd like to work with this package. 

The template configuration file looks like the following: ::

    # Project settings - aluminium$/srv/org.wikimedia.community-analytics/community-analytics
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

Once the installation is complete and the configuration has been set the modules can be imported into the Python environment.  The available operational modules are the following: ::

    e3_analysis.src.etl.data_loader
    e3_analysis.src.etl.log_parser
    e3_analysis.src.etl.data_filter
    e3_analysis.src.etl.experiments_loader
    e3_analysis.src.etl.timestamp_processor
    e3_analysis.src.etl.wpapi

    e3_analysis.src.metrics.blocks
    e3_analysis.src.metrics.bytes_added
    e3_analysis.src.metrics.edit_count
    e3_analysis.src.metrics.edit_rate
    e3_analysis.src.metrics.revert_rate
    e3_analysis.src.metrics.time_to_threshold
    e3_analysis.src.metrics.user_metric




