
User Metrics Definitions
========================

This package handles the logic involved with extracting metrics and
coordinating operations on those metrics.  These user metrics are specific
to Wikipedia users, but have the potential to be extended to other online
communites.

The definitions of what a user metric represents can be found in the
`research metrics documentation`_.  These defintions are manifest in the class
definitions in ``UserMetric`` derived classes.  The ``query`` subpackage defines
the details behind how the data is extracted from the MediaWiki backend data
stores.  The metrics manager module handles coordinating requests that require
the handling of several UserMetric objects.

.. _research metrics documentation: http://meta.wikimedia.org/wiki/Research:Metrics


Metrics Manager Module
-----------------------

.. automodule:: user_metrics.metrics.metrics_manager
   :members:

Users Module
------------

.. automodule:: user_metrics.metrics.users
   :members:

User Metrics Classes
====================

This set of module definitions are based on Wikimedia Foundation user metrics_
that are used to perform measurements primarily on new users in an effort to
better understand the evolving Wikipedia communities.

.. _metrics: http://meta.wikimedia.org/wiki/Research:Metrics

UserMetric Module
-----------------

.. automodule:: user_metrics.metrics.user_metric
   :members:


BytesAdded Module
-----------------

.. automodule:: user_metrics.metrics.bytes_added
   :members:

Blocks Module
-------------

.. automodule:: user_metrics.metrics.blocks
   :members:

Edit Count Module
-----------------

.. automodule:: user_metrics.metrics.edit_count
   :members:

Edit Rate Module
----------------

.. automodule:: user_metrics.metrics.edit_rate
   :members:

Live Account Module
-------------------

.. automodule:: user_metrics.metrics.live_account
   :members:

Namespace of Edits Module
-------------------------

.. automodule:: user_metrics.metrics.namespace_of_edits
   :members:

Survival Module
---------------

.. automodule:: user_metrics.metrics.survival
   :members:

Threshold Module
----------------

.. automodule:: user_metrics.metrics.threshold
   :members:

TimeToThreshold Module
----------------------

.. automodule:: user_metrics.metrics.time_to_threshold
   :members:

RevertRate Module
-----------------

.. automodule:: user_metrics.metrics.revert_rate
   :members:
