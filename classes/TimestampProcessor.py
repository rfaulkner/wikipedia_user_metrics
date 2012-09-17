
"""

This module effectively functions as a Singleton class.

TimestampProcesser facilitates the processing of timestamps used in the CiviCRM and "faulkner" mySQL 
databases.  This includes mapping among timestamp formats and converting those formats to indexed
lists and dictionaries.

Examples of format definitions:

    format 1 - 20080101000606
    format 2 - 2008-01-01 00:06:06   
    
Examples of resolution definitions:

    resolution 0 - xxxx-xx-xx 00:00:00
    resolution 1 - xxxx-xx-xx xx:00:00
    resolution 2 - xxxx-xx-xx xx:xx:00
    resolution 3 - xxxx-xx-xx xx:xx:xx

METHODS:

    normalize_timestamps          - Takes a list of timestamps as input and converts it to a set of days, hours, or minutes counting back from 0
    timestamps_to_dict            - Convert lists into dictionaries before processing it is assumed that lists are composed of only simple types
    find_latest_date_in_list      - Find the latest time stamp in a list
    find_earliest_date_in_list    - Find the earliest time stamp in a list
    gen_date_strings              - Given a datetime object produce a timestamp a number of hours in the past and according to a particular format 
    timestamp_from_obj            - Convert datetime objects to a timestamp of a given format. 
    timestamp_to_obj              - Convert timestamp to a datetime object of a given format 
    normalize_intervals           - Inserts missing interval points into the time and metric lists
    timestamp_convert_format      - Converts from one timestamp format to another timestamp format 

"""

__author__ = "Ryan Faulkner"
__revision__ = "$Rev$"
__date__ = "April 8th, 2011"


import datetime, calendar as cal, math, re, logging, sys
import classes.Helper as mh

""" CONFIGURE THE LOGGER """
LOGGING_STREAM = sys.stderr
logging.basicConfig(level=logging.DEBUG, stream=LOGGING_STREAM, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%b-%d %H:%M:%S')


TS_FORMAT_FLAT = 1
TS_FORMAT_FORMAT1 = 2

"""  Time unit indices"""
HOUR = 1
DAY =  0


"""
    Get timestamps for interval
    
    INPUT:
         start_time_obj       - a datetime object indicating the start of the interval
"""
def timestamps_for_interval(start_time_obj, timestamp_format, **kwargs):
   
    resolution = 3
    if 'resolution' in kwargs:
        if isinstance(kwargs['resolution'], int) and kwargs['resolution'] >= 0 and kwargs['resolution'] <= 3:
            resolution = kwargs['resolution']
    
    for key in kwargs:
        
        if key == 'minutes':
            end_time_obj = start_time_obj + datetime.timedelta(minutes=kwargs[key])
        elif key == 'hours':
            end_time_obj = start_time_obj + datetime.timedelta(hours=kwargs[key])
        
        start_timestamp = timestamp_from_obj(start_time_obj, timestamp_format, resolution)
        end_timestamp = timestamp_from_obj(end_time_obj, timestamp_format, resolution)
         
        return [start_timestamp, end_timestamp]
   
   
"""
 
     Takes a list of timestamps as input and converts it to a set of days, hours, or minutes counting back from 0
     
     INPUT:
         time_lists       - a list of datetime objects
         count_back       - indicate whether the list counts back from the end
         time_unit        - an integer indicating what unit to measure time in (0 = day, 1 = hour, 2 = minute)
     
     RETURN: 
         time_norm        - a dictionary of normalized times
         
"""
def normalize_timestamps(time_lists, count_back, time_unit):
    
    """ Convert timestamps if they are strings """
    if isinstance(time_lists, list):
        time_lists = timestamp_list_to_obj(time_lists)
    elif isinstance(time_lists, dict):
        time_lists = timestamp_dict_to_obj(time_lists)
    else:
        logging.error('TimestampProcessor::normalize_timestamps -- Timestamps must be contained in a list or dictionary.')
        return dict()
    
    time_lists, isList = timestamps_to_dict(time_lists)
    
    """ Depending on args set the start date """
    if count_back:
        start_date_obj = find_latest_date_in_list(time_lists)
    else:
        start_date_obj = find_earliest_date_in_list(time_lists)
    
    # Normalize dates
    time_norm = mh.AutoVivification()
    for key in time_lists.keys():
        for date_obj in time_lists[key]:
                        
            td = date_obj - start_date_obj
            diff = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6 # compute the number of seconds in the difference
            
            if time_unit == 0:                
                elem = diff / (60 * 60 * 24)     # Time difference in days
            elif time_unit == 1:                
                elem = diff / (60 * 60)          # Time difference in hours
            elif time_unit == 2 or time_unit == 3:
                elem = diff / 60                 # difference in minutes

            try: 
                time_norm[key].append(elem)
            except:
                time_norm[key] = list()
                time_norm[key].append(elem)
    
    """ If the original argument was a list put it back in that form """
    if isList:
        time_norm = time_norm[key]
        
    return time_norm
     

"""

    HELPER METHOD for normalize_timestamps.  Convert lists into dictionaries before processing it is assumed that lists 
    are composed of only simple types
    
    INPUT:
        time_lists    - a list of datetime objects
        
    RETURN: 
        time_lists    - dictionary with a single key 'key' that stores the list
        isList        - a dictionary of normalized times
        
"""
def timestamps_to_dict(time_lists):
    
    isList = 0
    if type(time_lists) is list:
        isList = 1
        
        old_list = time_lists
        time_lists = mh.AutoVivification()
        
        key = 'key'
        time_lists[key] = list()
        
        for i in range(len(old_list)):
            time_lists[key].append(old_list[i])

    return [time_lists, isList]

       
"""

    HELPER METHOD for normalize_timestamps.  Find the latest time stamp in a list
    
    INPUT:
        time_lists       - a list of datetime objects
        
    RETURN: 
        date_max        - datetime object of the latest date in the list
        
"""
def find_latest_date_in_list(time_lists):
    
    date_max = datetime.datetime(1000,1,1,0,0,0)
    
    for key in time_lists.keys():
        for date_obj in time_lists[key]:            
            if date_obj > date_max:
                date_max = date_obj
                
    return date_max

"""

    HELPER METHOD for normalize_timestamps.  Find the earliest timestamp in a list
    
    INPUT:
        time_lists       - a list of datetime objects
        
    RETURN: 
        date_min        - datetime object of the earliest date in the list
        
"""
def find_earliest_date_in_list(time_lists):
    
    date_min = datetime.datetime(3000,1,1,0,0,0)
    
    for key in time_lists.keys():
        for date_obj in time_lists[key]:
            if date_obj < date_min:
                date_min = date_obj
                
    return date_min


"""

    Given a datetime object produce a timestamp a number of hours in the past and according to a particular format
    
    format 1 - 20080101000606
    format 2 - 2008-01-01 00:06:06
    
    INPUT:
    
        now              - datetime object
        hours_back       - the amount of time the 
        format           - the format of the returned timestamp strings 
        resolution       - the resolution detail of the timestamp (e.g. down to the minute, down to the hour, ...)
    
    
    RETURN:
         start_time     - formatted datetime string
         end_time       - formatted datetime string
    
"""
def gen_date_strings(time_ref, hours_back, format, resolution):
    
    delta = datetime.timedelta(hours=-hours_back)

    time_obj = time_ref + delta
    time_ref = time_ref + datetime.timedelta(hours=-1) # Move an hour back to terminate at 55 minute
    
    # Cast the start and end time strings in the proper format
    start_time = timestamp_from_obj(time_obj, format, resolution)
    end_time = timestamp_from_obj(time_ref, format, resolution)

    return [start_time, end_time]



"""

    Convert datetime objects to a timestamp of a given format.  HELPER METHOD for gen_date_strings.
        
    INPUT:
    
        time_obj         - datetime object
        format           - the format of the returned timestamp strings 
        resolution       - the resolution detail of the timestamp (e.g. down to the minute, down to the hour, ...)
    
    
    RETURN:
         start_time     - formatted datetime string
         end_time       - formatted datetime string
         
         
    Examples of format definitions:

        format 1 - 20080101000606
        format 2 - 2008-01-01 00:06:06   
        
    Examples of resolution definitions:
    
        resolution 0 - xxxx-xx-xx 00:00:00
        resolution 1 - xxxx-xx-xx xx:00:00
        resolution 2 - xxxx-xx-xx xx:xx:00
        resolution 3 - xxxx-xx-xx xx:xx:xx

"""
def timestamp_from_obj(time_obj, format, resolution):
    
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
            timestamp = str(time_obj.year) + month + day + '000000'
        elif resolution == 1:
            timestamp = str(time_obj.year) + month + day + hour + '0000'
        elif resolution == 2:
            timestamp = str(time_obj.year) + month + day + hour + minute + '00'
        elif resolution == 3:
            timestamp = str(time_obj.year) + month + day + hour + minute + second
    
    elif format == 2:
        
        if resolution == 0:
            timestamp = str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  '00:00:00'
        elif resolution == 1:
            timestamp = str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  hour + ':00:00'
        elif resolution == 2:
            timestamp = str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  hour + ':' + minute + ':00'
        elif resolution == 3:
            timestamp = str(time_obj.year) + '-' +  month + '-' +  day + ' ' +  hour + ':' + minute + ':' + second
            
    return timestamp


"""

    Convert timestamp to a datetime object of a given format
    
    INPUT:
        timestamp        - timestamp string
        format           - the format of the returned timestamp strings 
    
    RETURN:
         time_obj     - datetime conversion of timestamp string
         
         
    Examples of format definitions:

        format 1 - 20080101000606
        format 2 - 2008-01-01 00:06:06   
     
"""
def timestamp_to_obj(timestamp, format):
    
    if format == 1:
        time_obj = datetime.datetime(int(timestamp[0:4]), int(timestamp[4:6]), int(timestamp[6:8]), \
                                    int(timestamp[8:10]), int(timestamp[10:12]), int(timestamp[12:14]))
        
    elif format == 2:
        time_obj = datetime.datetime(int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10]), \
                                    int(timestamp[11:13]), int(timestamp[14:16]), int(timestamp[17:19]))

    return time_obj

"""
    Convert a list timestamps to a list  datetime objects of a given format

    @param timestamps: List of string timestamps
    @param format: Timestamp formats
"""
def timestamp_list_to_obj(timestamps):
    
    if not(isinstance(timestamps, list)):
        logging.error('TimestampProcessor::timestamp_list_to_obj -- Parameter must be a list of timestamps.')
        return list()
    
    obj_list = list()
    
    for ts in timestamps:
        if isinstance(ts, str):            
            format = getTimestampFormat(ts)        
            obj_list.append(timestamp_to_obj(ts, format))
        else:
            obj_list.append(ts)
            
    return obj_list

""" Same as method above for dictionaries containing timestamp lists """
def timestamp_dict_to_obj(timestamps_dict):
    
    if not(isinstance(timestamps_dict, dict)):
        logging.error('TimestampProcessor::timestamp_dict_to_obj -- Parameter must be a dictionary of timestamps.')
        return dict()
    
    """ Use timestamp_list_to_obj method to do the work """
    for key in timestamps_dict:
        timestamps_dict[key] = timestamp_list_to_obj(timestamps_dict[key])
    
    return timestamps_dict

        
"""

    Evaluates a timestamp of a given format to ensure that it is valid
    
    INPUT:
        timestamp        - timestamp string
        format           - the format of the returned timestamp strings 
    
    RETURN:
         boolean result
         
         
    Examples of format definitions:

        format 1 - 20080101000606
        format 2 - 2008-01-01 00:06:06   
     
"""
def is_timestamp(timestamp, format):

    try:
        if format == 1:
            if len(timestamp) == 14 and unicode(timestamp).isnumeric():
                return True
            else:
                return False
            
        elif format == 2:
            if len(timestamp) == 19 and unicode(timestamp[0:4]).isnumeric() and unicode(timestamp[5:7]).isnumeric() and unicode(timestamp[8:10]).isnumeric() and \
                unicode(timestamp[11:13]).isnumeric() and unicode(timestamp[14:16]).isnumeric() and unicode(timestamp[17:19]).isnumeric():
                return True
            else:
                return False
    except:
        return False
    



"""

    Inserts missing interval points into the time and metric lists
    
    Assumptions: 
        _metrics_ and _times_ are lists of the same length
        there must be a data point at each interval 
        Some data points may be missed
        where there is no metric data the metric takes on the value 0.0
    
    e.g. when _interval_ = 10
    times = [0 10 30 50], metrics = [1 1 1 1] ==> [0 10 30 40 50], [1 1 0 1 0 1]    
    
    INPUT:
    
        times           - 
        metrics         -  
        interval        -
    
    RETURN:
         new_times     - 
         new_metrics   -
    
"""
def normalize_intervals(times, metrics, interval):
        
    current_time = 0.0
    index = 0
    iterations = 0
    max_elems = math.ceil((times[-1] - times[0]) / interval) # there should be no more elements in the list than this
    
    new_times = list()
    new_metrics = list()
    
    """ Iterate through the time list """
    while index < len(times):
        
        """ TEMPORARY SOLUTION: break out of the loop if more than the maximum number of elements is reached """
        if iterations > max_elems:
            break;
        
        new_times.append(current_time)
        
        """ If the current time is not in the current list then add it and a metric value of 0.0
            otherwise add the existing elements to the new lists """
        if current_time != times[index]:
            new_metrics.append(0.0)
        
        else:
            new_metrics.append(metrics[index])
            index = index + 1
        
        current_time = current_time + interval
        
        iterations = iterations + 1

    return [new_times, new_metrics]

"""

    Converts from one timestamp format to another timestamp format
        
    format 1 - 20080101000606
    format 2 - 2008-01-01 00:06:06    
        
    INPUT:
    
        ts           - timestamp string
        format_from  - input format
        format_to    - output format
    
    RETURN:
    
         new_timestamp     - new timestamp string
     

"""
def timestamp_convert_format(ts, format_from, format_to, **kwargs):
    
    format_from = getTimestampFormat(ts)
    
    if format_from == 1:        
        if format_to == 1:
            new_timestamp = ts
        elif format_to == 2:
            new_timestamp = ts[0:4] + '-' + ts[4:6] + '-' + ts[6:8] + ' ' + ts[8:10] + ':' + ts[10:12] + ':' + ts[12:14]
            
    elif format_from == 2:
        if format_to == 1:
            new_timestamp = ts[0:4] + ts[5:7] + ts[8:10] + ts[11:13] + ts[14:16] + ts[17:19]
        elif format_to == 2:
            new_timestamp = ts
             
    return new_timestamp

"""

    THIS METHOD IS CURRENTLY COUPLED WITH HYPOTHESIS TEST
    
    This method takes a start time and endtime and interval length and produces
    a list of timestamps corresponding to each time spaced by length 'interval'
    between the start time and the end time inclusive
    
    INPUT:
        num_samples      - number of samples per test interval
        interval         - intervals at which samples are drawn within the range, units = minutes
        start_time       - start timestamp 'yyyymmddhhmmss'
        end_time         - end timestamp 'yyyymmddhhmmss'
        format           - !! CURRENTLY UNUSED !! specifies timestamp format
        
    RETURN:
        
        times            - list of timestamps for each sample
        time_indices     - list of indices counting from zero marking the indices for reporting test interval parameters

"""
def get_time_lists(start_time, end_time, interval, num_samples, ts_format):

    """ range must be divisible by interval - convert to hours """
    list_range = float(interval * num_samples) / 60
    
    """ Compose times """
    start_datetime = datetime.datetime(int(start_time[0:4]), int(start_time[4:6]), int(start_time[6:8]), int(start_time[8:10]), int(start_time[10:12]), int(start_time[12:14]))
    end_datetime = datetime.datetime(int(end_time[0:4]), int(end_time[4:6]), int(end_time[6:8]), int(end_time[8:10]), int(end_time[10:12]), int(end_time[12:14]))

    """ current timestamp and hour index """
    curr_datetime = start_datetime
    curr_timestamp = start_time
    curr_hour_index = 0.0
    
    """ lists to store timestamps and indices """
    times = []
    time_indices = []

    sample_count = 1
    
    """ build a list of timestamps and time indices for plotting increment the time """
    while curr_datetime < end_datetime:
        
        """ for timestamp formatting """
        month_str_fill = ''
        day_str_fill = ''
        hour_str_fill = ''
        minute_str_fill = ''
        if curr_datetime.month < 10:
            month_str_fill = '0'
        if curr_datetime.day < 10:
            day_str_fill = '0'
        if curr_datetime.hour < 10:
            hour_str_fill = '0'
        if curr_datetime.minute < 10:
            minute_str_fill = '0'
        
        curr_timestamp = str(curr_datetime.year) + month_str_fill + str(curr_datetime.month) + day_str_fill + str(curr_datetime.day) + hour_str_fill+ str(curr_datetime.hour) + minute_str_fill+ str(curr_datetime.minute) + '00'
        times.append(curr_timestamp)
        
        """ increment curr_hour_index if the """
        if sample_count == num_samples: 
            
            time_indices.append(curr_hour_index + list_range / 2)
            curr_hour_index = curr_hour_index + list_range
            sample_count = 1
        else:
            sample_count = sample_count + 1 
                
            
        """ increment the time by interval minutes """
        td = datetime.timedelta(minutes=interval)
        curr_datetime = curr_datetime + td
    
    """ append the last items onto time lists """
    times.append(end_time)
        
    return [times, time_indices]



""" Determines the following hour based on the precise date to the hour """
def getNextHour(year, month, day, hour):

    lastDayofMonth = cal.monthrange(year,month)[1]

    next_year = year
    next_month = month
    next_day = day
    next_hour = hour + 1

    if hour == 23:
        next_hour = 0
        if day == lastDayofMonth:
            next_day = 1
            if month == 12:
                next_month = 1
                next_year = year + 1

    return [next_year, next_month, next_day, next_hour]

""" Determines the previous hour based on the precise date to the hour """
def getPrevHour(year, month, day, hour):
    
    if month == 1:
        last_year = year - 1
        last_month = 12
    else:
        last_year = year
        last_month = month - 1
        
    lastDayofPrevMonth = cal.monthrange(year,last_month)[1]
        
    prev_year = year
    prev_month = month
    prev_day = day
    prev_hour = hour - 1

    if prev_hour == -1:
        prev_hour = 23
        if day == 1:
            prev_day = lastDayofPrevMonth
            prev_month = last_month
            prev_year = last_year
        else:
            prev_day = day - 1
            
    return [prev_year, prev_month, prev_day, prev_hour]

""" 
    Given a timestamp infer it's format using regular expression matching
"""
def getTimestampFormat(timestamp):
    
    if re.search('[0-9]{14}', timestamp):
        return 1
    elif re.search('.*-.*-.* .*:.*:.*', timestamp):
        return 2
    else: 
        return -1


""" 
    Create timestamp list 
    
    @param start: start time stamp - datetime obj
"""
def create_timestamp_list(start, num_samples, interval_minutes):
    
    curr_obj = start
    ts_list = [timestamp_from_obj(curr_obj, 1, 3)]
        
    delta = datetime.timedelta(minutes=interval_minutes)
    
    for i in range(num_samples - 1):
        curr_obj = curr_obj + delta
        ts_list.append(timestamp_from_obj(curr_obj, 1, 3))
        
    return ts_list

"""

    Change the resolution of the timestamp

    format 1 - 20080101000606
    format 2 - 2008-01-01 00:06:06   
    
    resolution 0 - xxxx-xx-xx 00:00:00
    resolution 1 - xxxx-xx-xx xx:00:00
    resolution 2 - xxxx-xx-xx xx:xx:00
    resolution 3 - xxxx-xx-xx xx:xx:xx

"""
def reduce_resolution(ts, new_resolution):
    
    ts_format = getTimestampFormat(ts)
    
    if ts_format == 1:
        if new_resolution == 0:
            ts = ts[:8] + '000000'
        elif new_resolution == 1:
            ts = ts[:10] + '0000'
        elif new_resolution == 2:
            ts = ts[:12] + '00'
    elif ts_format == 2:
        if new_resolution == 0:
            ts = ts[:11] + '00:00:00'
        elif new_resolution == 1:
            ts = ts[:14] + '00:00'
        elif new_resolution == 2:
            ts = ts[:17] + '00'

    return ts

