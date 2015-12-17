#!/usr/bin/env python
"""
This file contains all util functions.
"""
import time
import os
import shutil
import json
from bisect import bisect_left

# time.ctime(0) is 'Wed Dec 31 16:00:00 1969'
TIME_INFOR = {
    "week": {
        "second_shift": 2 * 24 * 60 * 60 + 16 * 60 * 60,
        "second_range": 7 * 24 * 60 * 60
    },
    "day": {
        "second_shift": 16 * 60 * 60,
        "second_range": 24 * 60 * 60
    },
    "hour": {
        "second_shift": 0,
        "second_range": 60 * 60
    },
    "debug": {
        "second_shift": 0,
        "second_range": 6 * 60
    }
}
TIME_PATTERN = '%m.%d.%Y %H:%M:%S'
RECORD_CONFIG_PATTERN = {
    'api_server': str,
    'record_token': str,
    'ts_server': str,
    'query': list,
    'data_directory': str,
    'start_time': str,
    'time_range': str,
    'data_file_interval': float
}
METADATA_FILE = 'metadata.json'
TS_DATA_DIR = 'ts_data'
CONFIG_FILE = 'configuration.json'
TAR_NAME = "replay-data.tar.gz"


class Error(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        print self.message


def convert_time_to_second(time_string):
    """
    This function is converting the time string like
    "YYYY.MM.DD-HH:MM:SS" to the second time.

    :param time_string: "MM.DD.YYYY HH:MM:SS"
    :return: second time
    """
    return int(time.mktime(time.strptime(time_string, TIME_PATTERN)))


def get_second_shift(current_time, time_range):
    """
    Get current time 'second shift'.
     'second shift' mean:
     For example, time range is 1 week.
     If current time is the Monday 00:05:01, the 'second shift' is 5 * 60 + 1
     'second shift' is used to find the old time series data.

    :param current_time: Current second time
    :param time_range: second time range
    :return: second time shift
    """

    # Find current time range number of current time.
    second_shift = TIME_INFOR[time_range]["second_shift"]
    second_range = TIME_INFOR[time_range]["second_range"]
    time_range_number = int((current_time + second_shift) / second_range)

    # Find first Monday 00:00:00 time second
    base_time = time_range_number * second_range - second_shift

    # return 'second shift' based on base_time
    return (current_time - base_time) % second_range


def get_end_time_of_slot(current_time, interval, time_range):
    """
    Get the end of current time interval.
    For example: if the current time is 12:00:01, interval is 30 min,
     the end time of this slot is 12:30:00
    end_time_of_slot = current_time - current_second_shift +
     (time_slot + 1) * interval

    :param current_time: current second time.
    :param interval: interval second
    :param time_range: time range
    :return : the end time of current interval
    """
    current_second_shift = get_second_shift(current_time, time_range)
    time_slot = get_time_slot_number(int(current_time), interval, time_range)
    return current_time - current_second_shift + (time_slot + 1) * interval


def get_time_slot_number(current_time, interval, time_range):
    """
    Get the time slot number in a time range.
    For example , if the time range is 2 weeks, interval is 10 minutes,
    The First Monday 00:00:01 is in the 1st time slot.
    The Second Tuesday 12:00:01 is in the [(8 * 24 + 12) * 6 + 1]th time slot.

    :param current_time: current second time
    :param interval: second interval
    :param time_range: second time range
    :return: the number of time slot
    """
    return int(get_second_shift(current_time, time_range) / interval)


def get_time_series_file_path(current_time, interval, time_range, folder_path,
                              suffix):
    # week_number = get_week_number(current_time, start_time)
    """
    Get the file name of the current time.
    The file name is 'the folder_path'/'time_slot number'+'.'+suffix

    :param time_range: second time range
    :param current_time: current second time
    :param interval: second interval
    :param folder_path: The folder path of the file
    :param suffix: suffix of time series file
    :return: the file path of current time
    """
    time_slot_number = get_time_slot_number(current_time, interval, time_range)
    base_file_name = str(folder_path + '/' + str(time_slot_number).zfill(5))
    return base_file_name + '.' + suffix


def get_next_time_series_file_path(current_path, interval, time_range):
    basename = os.path.basename(current_path)
    basename_array = basename.split('.')
    next_slot_number = (int(basename_array[0]) + 1) %\
                       (int(TIME_INFOR[time_range]['second_range'] / interval))
    return str(os.path.dirname(current_path)
               + '/' + str(next_slot_number).zfill(5)
               + '.' + basename_array[1])


def get_new_interval_information(time_series, interval, time_range):
    """
    This function is to get the second time shift information and next index
    information when the new file appear.

    :param time_series: Sorted time points
    :param interval: second interval of each file
    :param time_range: second time range
    :return: current_second_shift: the second shift for the first time
    :return: next_index: index of next data
    """
    # Get current time slot number
    current_time_slot_number = get_time_slot_number(time.time(), interval,
                                                    time_range)

    # Get next time slot number
    next_time_slot_number = get_time_slot_number(time_series[0], interval,
                                                 time_range)

    # Get second shift
    current_second_shift = get_second_shift(time.time(), time_range)

    # Get next index
    next_index = bisect_left(time_series, current_second_shift)

    # If current time slot is the last one and next_time_slot_number
    #  is first one.

    last_slot_number = int(TIME_INFOR[time_range]['second_range'] / interval)-1

    if next_index == len(time_series) and \
            current_time_slot_number == last_slot_number and \
            next_time_slot_number == 0:
        current_second_shift -= TIME_INFOR[time_range]['second_range']
        next_index = 0

    return current_second_shift, next_index


def create_folder_path(folder_path):
    """
    Delete the old folder_path and create a new folder by path

    :param folder_path: Folder path

    """
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
        os.makedirs(folder_path)
    except Exception:
        raise Error('Create {folder_path} exception'.format(folder_path))


def remove_double_quotes(string):
    if string[0] == '"' and string[len(string) - 1] == '"':
        return string[1:len(string) - 1]
    return string


def create_path(base_path, sub_path):
    return base_path + '/' + sub_path


def check_record_config(config):
    """
    This function is to check the record configuration.
    :param config: config dictionary
    """

    def convert_type(item, convert_func):
        if item not in config.keys():
            raise Error("Config file dose not has '{0}'!".format(item))
        try:
            record_dict[item] = convert_func(config[item])
        except Exception:
            raise Error("Config['{0}'] is not correct!".format(item))

    def check_time_range():
        if record_dict['time_range'] not in TIME_INFOR.keys():
            raise Error("Time range is not {0}".format(TIME_INFOR.keys()))

    def check_query():
        try:
            record_dict['query'] = [str(item) for item in config['query']]
        except Exception:
            raise Error("Config['query'] is not correct!")

    def check_start_time():
        try:
            record_dict['start'] = convert_time_to_second(config['start_time'])
        except Exception:
            raise Error("Config['start_time'] pattern is not: {0}".format(
                TIME_PATTERN))

        second_range = TIME_INFOR[record_dict['time_range']]['second_range']
        record_dict['end'] = record_dict['start'] + second_range
        if record_dict['end'] > time.time():
            raise Error("Config start time + time interval > now")

    record_dict = {}
    for item_key, item_type in RECORD_CONFIG_PATTERN.items():
        convert_type(item_key, item_type)
    check_query()
    check_time_range()
    check_start_time()

    record_dict['metadata_path'] = \
        create_path(record_dict['data_directory'], METADATA_FILE)
    record_dict['ts_directory'] = \
        create_path(record_dict['data_directory'], TS_DATA_DIR)
    record_dict['record_config'] = \
        create_path(record_dict['data_directory'], CONFIG_FILE)
    record_dict['interval'] = record_dict['data_file_interval'] * 60 * 60

    return record_dict


def read_record_config(config_file):
    """
    Check if record config file is a valid json file
    :param config_file:
    :return: json content of config file
    """
    if not os.path.isfile(config_file):
        raise Error('Configuration file : \'{0}\''
                    ' dose not exist'.format(config_file))
    try:
        with open(config_file, 'r') as input_file:
            json_data = input_file.read()
        return json.loads(json_data)
    except Exception:
        raise Error("Configuration file \'{0}\' "
                    "is not a valid json file".format(config_file))


def check_data_dir(data_dir):
    """
    Check if the data directory is complete.
    :param data_dir: record data directory
    """
    if not os.path.exists(data_dir):
        raise Error('Data directory {0} dose not exist!'.format(data_dir))
    config_file = os.path.isfile(create_path(data_dir, CONFIG_FILE))
    meta_data_file = os.path.isfile(create_path(data_dir, METADATA_FILE))
    ts_data_dir = os.path.exists(create_path(data_dir, TS_DATA_DIR))
    ts_not_file = not os.path.isfile(create_path(data_dir, TS_DATA_DIR))
    if not (config_file and meta_data_file and ts_data_dir and ts_not_file):
        raise Error('Data directory is not complete!')
