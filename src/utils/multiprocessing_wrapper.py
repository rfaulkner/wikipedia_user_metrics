
"""
    This module provides a set of methods for handling multi-threading patterns more easily.

    >>> import src.utils.multiprocessing_wrapper as mpw
    >>> mpw.build_thread_pool(['one','two'],len,2,[])
    [2,2]
"""

import multiprocessing as mp
import math

__author__ = "ryan faulkner"
__date__ = "12/12/2012"
__license__ = "GPL (version 2 or later)"

def build_thread_pool(data, callback, k, args):
    """
        Handles initializing, executing, and cleanup for thread pools
    """

    # partition data
    n = int(math.ceil(float(len(data)) / k))
    arg_list = list()
    for i in xrange(k):
        arg_list.append([data[i * n : (i + 1) * n], args])
    pool = mp.Pool(processes=len(arg_list))

    # Call worker threads and aggregate results
    results = list()
    for elem in pool.map(callback, arg_list):
        if hasattr(elem, '__iter__'):
            results.extend(elem)
        else:
            results.extend([elem])

    try:
        pool.close()
    except RuntimeError:
        pass
    return results