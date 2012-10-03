
__author__ = "Ryan Faulkner"
__date__ = "October 3rd, 2012"
__license__ = "GPL (version 2 or later)"

import sys
import logging
import DataFilter as DF

# CONFIGURE THE LOGGER
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


class TemplateLinksPTFilter(DF.DataFilter):
    """
        Parses lines from PT wiki "what links here"

        The mutable object is the string to be parsed as a line from the link source
    """

    def __init__(self, **kwargs):
        """
            The base constructor supplies all that is needed
        """
        DF.DataFilter.__init__(self, **kwargs)


    def execute(self, **kwargs):
        """
            Removes all keys not in the key list and adds a default list of times for those keys missing (however this should generally not occur)
        """

        bail = False
        if 'str_to_parse' in kwargs.keys():
            str_to_parse = kwargs['str_to_parse']

            if not isinstance(str_to_parse,str):
                bail = True
        else:
            bail = True

        if bail:
            logging.error('TemplateLinksPTFilter :: execute.  An input string must be provided.')
            return

        left_pattern = 'A3o:'
        right_pattern = '"}}{'

        try:
            p1 = str_to_parse.split(left_pattern)[1]
            p2 = p1.split(right_pattern)[0]

        except:
            return ''

        return p2