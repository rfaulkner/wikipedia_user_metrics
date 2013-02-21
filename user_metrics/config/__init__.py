
# CONFIGURE THE LOGGER
import logging
import sys
logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%b-%d %H:%M:%S')