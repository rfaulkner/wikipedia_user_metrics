
"""
    This module provides a set of methods for handling multi-threading
    patterns more easily. ::

        >>> import user_metrics.utils.multiprocessing_wrapper as mpw
        >>> mpw.build_thread_pool(['one','two'],len,2,[])
        [2,2]
"""

import multiprocessing as mp
import multiprocessing.pool as mp_pool
import math

__author__ = "ryan faulkner"
__date__ = "12/12/2012"
__license__ = "GPL (version 2 or later)"


def build_thread_pool(data, callback, k, args):
    """
        Handles initializing, executing, and cleanup for thread pools. Given
        the iterable ``data`` and a thread count ``k`` partition the data and
        execute ``k`` independent jobs on ``callback`` with ``args`` passed.
        Finally combine the results of each job.
    """

    # partition data
    n = int(math.ceil(float(len(data)) / k))
    arg_list = list()

    for i in xrange(k):
        arg_list.append([data[i * n : (i + 1) * n], args])

    # remove any args with empty revision lists
    arg_list = filter(lambda x: len(x[0]), arg_list)
    if not arg_list: return []

    pool = NonDaemonicPool(processes=len(arg_list))
    results = list()
    # Call worker threads and aggregate results
    if arg_list:
        for elem in pool.map(callback, arg_list):
            if hasattr(elem, '__iter__'):
                results.extend(elem)
            else:
                results.extend([elem])
    pool.terminate()
    return results


class NoDaemonicProcess(mp.Process):
    """
        Sub-classes multiporcessing.Process always making the 'daemon'
        attribute return False.  This is useful when a process needs to
        be assigned a parent Process and thus must not be 'daemonic'.
    """

    # From http://st ackoverflow.com/questions/6974695/
    #                               python-process-pool-non-daemonic
    # courtesy of stackoverflow user Chris Arndt - chrisarndt.de

    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)


class NonDaemonicPool(mp_pool.Pool):
    """
        Sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
        because the latter is only a wrapper function, not a proper class.
    """
    Process = NoDaemonicProcess