
"""
    Defines a Singleton logger class for the user metrics project.  This can
    be used to set different log handlers and verbosity.
"""

__author__ = 'ryan faulkner'
__email__ = 'rfaulkner@wikimedia.org'
__date__ = "02-21-2013"
__license__ = "GPL (version 2 or later)"


import logging
import sys


class UMLogBuilder(object):

    __instance = None   # Singleton instance

    def __init__(self, **kwargs):
        """ Constructor - Initialize class members and initialize
            logger object """
        self.__class__.__instance = self
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(__name__)

    def __new__(cls, *args, **kwargs):
        """ This class is Singleton, return only one instance """
        if not cls.__instance:
            cls.__instance = super(UMLogBuilder, cls).__new__(cls, *args,
                **kwargs)
        return cls.__instance

    def set_logger(self, args, out=None, err=None):
        """ Set log handler and verbosity. """
        if out is None:
            out = sys.stdout
        if err is None:
            err = sys.stderr

        level = logging.WARNING - ((args.verbose - args.quiet) * 10)
        if args.silent:
            level = logging.CRITICAL + 1

        log_format = "%(asctime)s %(levelname)-8s %(message)s"
        handler = logging.StreamHandler(out)
        handler.setFormatter(logging.Formatter(fmt=log_format,
            datefmt='%b-%d %H:%M:%S'))
        self._logger.addHandler(handler)
        self._logger.setLevel(level)
