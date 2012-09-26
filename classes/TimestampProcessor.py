
"""

This module effectively functions as a Singleton class that provides some basic operations on timestamp strings and python
datetime.datetime objects.

Throughout the class definition reference is made to the concept of timestamp formats and resolutions.  Formats define the
formatting of the timestamp string while resolution indicates the degree of information contained in the representation.  Below
are the definitions: ::


    Format 1 - 20080101000606
    Format 2 - 2008-01-01 00:06:06
    
    *('x' indicates that the value is variable)*

    Resolution 0 - xxxx-xx-xx 00:00:00
    Resolution 1 - xxxx-xx-xx xx:00:00
    Resolution 2 - xxxx-xx-xx xx:xx:00
    Resolution 3 - xxxx-xx-xx xx:xx:xx

"""

__author__ = "Ryan Faulkner"
__date__ = "April 8th, 2011"


import datetime
import re
import logging
import sys

# CONFIGURE THE LOGGER
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


TS_FORMAT_FLAT = 1
TS_FORMAT_FORMAT1 = 2

#  Time unit indices
HOUR = 1
DAY =  0


def timestamp_to_obj(timestamp, format):
    """
        Convert timestamp to a datetime object of a given format

            - Parameters:
                - *timestamp* - String.  Timestamp string
                - *format* - Integer.  Format index

            - Return:
                - datetime.datetime.  Datetime module representation of the timestamp
    """

    if format == 1:
        return datetime.datetime(int(timestamp[0:4]), int(timestamp[4:6]), int(timestamp[6:8]),int(timestamp[8:10]), int(timestamp[10:12]), int(timestamp[12:14]))

    elif format == 2:
        return datetime.datetime(int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]),int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))

    else:
        msg = 'TimestampProcessor::timestamp_to_obj - Could not resolve timestamp format.'
        logging.error(msg)
        raise TimestampProcessorException(msg)


def timestamp_from_obj(time_obj, format, resolution):
    """

        Convert datetime objects to a timestamp of a given format

            - Parameters:
                - *time_obj* - datetime.datetime.  Timestamp object
                - *format* - Integer.  Format index
                - *resolution* - Integer.  Resolution index

            - Return:
                - datetime.datetime.  Datetime module representation of the timestamp
    """

    if time_obj.month < 10:
        month = '0' + str(time_obj.month)
    else:
        month = str(time_obj.month)

    if time_obj.day < 10:
        day = '0' + str(time_obj.day)
    else:
        day = str(time_obj.day)

    if time_obj.hour < 10:
        hour = '0' + str(time_obj.hour)
    else:
        hour = str(time_obj.hour)

    if time_obj.minute < 10:
        minute = '0' + str(time_obj.minute)
    else:
        minute = str(time_obj.minute)

    if time_obj.second < 10:
        second = '0' + str(time_obj.second)
    else:
        second = str(time_obj.second)

    # Cast the start and end time strings in the proper format
    if format == 1:

        if resolution == 0:
            return str(time_obj.year) + month + day + '000000'
        elif resolution == 1:
            return str(time_obj.year) + month + day + hour + '0000'
        elif resolution == 2:
            return str(time_obj.year) + month + day + hour + minute + '00'
        elif resolution == 3:
            return str(time_obj.year) + month + day + hour + minute + second

    elif format == 2:

        if resolution == 0:
            return str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  '00:00:00'
        elif resolution == 1:
            return str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  hour + ':00:00'
        elif resolution == 2:
            return str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  hour + ':' + minute + ':00'
        elif resolution == 3:
            return str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  hour + ':' + minute + ':' + second

    else:
        msg = 'TimestampProcessor::timestamp_from_obj - Could not resolve timestamp format and resolution.'
        logging.error(msg)
        raise TimestampProcessorException(msg)


def timestamp_convert_format(timestamp, format_from, format_to, **kwargs):
    """
        Converts from one timestamp format to another timestamp format

            - Parameters:
                - *timestamp* - String.  Timestamp string
                - *format_from* - Integer.  Format index of input timestamp
                - *format_to* - Integer.  Format index of output timestamp

            - Return:
                - String.  Timestamp string in new format
    """

    format_from = getTimestampFormat(timestamp)

    if format_from == 1:
        if format_to == 1:
            return timestamp
        elif format_to == 2:
            new_timestamp = timestamp[0:4] + '-' + timestamp[4:6] + '-' + timestamp[6:8] + ' ' + timestamp[8:10] + ':' + timestamp[10:12] + ':' + timestamp[12:14]
            return new_timestamp

    elif format_from == 2:
        if format_to == 1:
            new_timestamp = timestamp[0:4] + timestamp[5:7] + timestamp[8:10] + timestamp[11:13] + timestamp[14:16] + timestamp[17:19]
            return new_timestamp
        elif format_to == 2:
            return timestamp

    else:
        msg = 'TimestampProcessor::timestamp_convert_format - Could not resolve timestamp format.'
        logging.error(msg)
        raise TimestampProcessorException(msg)


def getTimestampFormat(timestamp):
    """
        Given a timestamp infer it's format using regular expression matching

            - Parameters:
                - *timestamp* - String.  Timestamp string

            - Return:
                - String.  Timestamp string in new format
    """

    if re.search('[0-9]{14}', timestamp):
        return 1
    elif re.search('.*-.*-.* .*:.*:.*', timestamp):
        return 2
    else:
        return -1

class TimestampProcessorException(Exception):
    """
        Custom exception for this module.
    """

    def __init__(self, value):
        Exception.__init__(self)
        self.value = value

    def __str__(self):
        return repr(self.value)