Overview
========

Extract-Transact-Load (ETL) modules are used to gather data from a source, optionally operate on that data, and then finally store the data for later use. 

Aggregator Module
-----------------

.. automodule:: user_metrics.etl.aggregator
   :members:

Aggregator Error Class
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: user_metrics.etl.aggregator.AggregatorError
   :members:

DataLoader Module
-----------------

.. automodule:: src.etl.data_loader

Creating a TSV from MySQL data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's often necessary or helpful to generate a tab separated file from the results of a MySQL query.  This is accomplished easily using the DataLoader class.  Assuming that the project home is on the python PATH the following python operations will generate out.tsv in the "data" folder under the project home: ::

    >>> from src.etl import data_loader as dl
    >>> d = dl.Handle(db='slave')
    >>> sql_string = 'select * from enwiki.user limit 200'
    >>> results = d.execute_SQL(sql_string)
    >>> d.dump_to_csv()
    Aug-02 18:43:32 INFO     Writing results to: <project_home>/data/out.tsv

Connector Class
~~~~~~~~~~~~~~~

.. autoclass:: user_metrics.etl.data_loader.Connector
    :members:

DataLoader Class
~~~~~~~~~~~~~~~~

.. autoclass:: user_metrics.etl.data_loader.DataLoader
   :members:


TableLoader Module
------------------

.. automodule:: user_metrics.etl.table_loader

TableLoader Class
~~~~~~~~~~~~~~~~~

.. autoclass:: user_metrics.etl.table_loader.TableLoader
   :members:

WPAPI Module
------------

.. automodule:: user_metrics.etl.wpapi

WPAPI Class
~~~~~~~~~~~

.. autoclass:: user_metrics.etl.wpapi.WPAPI
   :members:
