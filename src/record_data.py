#!/usr/bin/env python
"""
This file implements all funcitons about download time series data and
convert raw time series data file to json time series data file.
"""
import json
import os
import glob
import tarfile
import urllib2
import urllib
import shutil
from tsdb import TsdbWrapper
from sf.timeseries.ttypes import MetricTimeSeriesRollup
from sf.id.ttypes import ID
from sf.timeseries.ttypes import TsdbException
import base64
import struct
from sf.datamodel.ttypes import RollupType
from src.util import Error
from src.util import TAR_NAME
from src.util import get_second_shift
from src.util import read_record_config
from src.util import check_record_config
from src.util import create_folder_path
from src.util import get_time_series_file_path


# WEEK_SECONDS = 7 * 24 * 60 * 60

ROLLUP = {
    "GAUGE": RollupType.AVERAGE_ROLLUP,
    "COUNTER": RollupType.SUM_ROLLUP,
    "CUMULATIVE_COUNTER": RollupType.MAX_ROLLUP
}


def base642long(base64str):
    """
    Convert string to long

    :param base64str: string
    :return: long result
    """
    result = struct.unpack(">q", base64.urlsafe_b64decode(base64str))[0]
    return result


def to_id(value):
    """
    Convert value to ID

    :param value: metric_id
    :return: ID
    """
    if isinstance(value, ID):
        return value
    try:
        return ID(int(value))
    except ValueError:
        if not value[-1] == '=':
            value += '='
        return ID(base642long(value))


def get_metadata(api_server, query_list, record_token):

    def filter_sf(raw_metadata):
        result = {}
        for key, value in raw_metadata.items():
            if not key.startswith('sf') and not key.startswith('_sf'):
                result[key] = value
        return result

    try:
        url_pattern = '{api_server}/v1/metrictimeseries?query={query}'
        raw_metadata_list = []
        metadata_dict = {}
        for query in query_list:
            query = urllib.quote(query)
            api_url = url_pattern.format(api_server=api_server, query=query)
            print api_url
            data = urllib2.Request(api_url,
                                   headers={'X-SF-TOKEN': record_token})
            metadata = json.loads(urllib2.urlopen(data).read())
            raw_metadata_list += metadata['rs']

        for raw_metadata in raw_metadata_list:
            metric_id = str(raw_metadata['sf_id'])
            metadata_dict[metric_id] = {
                'sf_metricType': raw_metadata['sf_metricType'],
                'sf_metric': raw_metadata['sf_metric'],
                'dimensions': filter_sf(raw_metadata)
            }
        return metadata_dict
    except Exception:
        raise Error("Cannot get metadata please check your api_server,"
                    " record token and query sentences")


def pull_ts_data_from_server(server, metric_id, start_time, end_time, rollup):
    """
    Get time series data by time and metric id

    :param server: Server Ip address of time series data server.
    :param metric_id: Metric id
    :param start_time: Start millisecond time
    :param end_time: End millisecond time
    :return: Time series data
    """
    tsdb = TsdbWrapper.TsdbWrapper(host=server)
    mtslds = [MetricTimeSeriesRollup(to_id(metric_id), ROLLUP[rollup])]
    return tsdb.getTimeSeriesByIds(mtslds,
                                   None,
                                   1000,
                                   start_time,
                                   end_time)


def write_ts_data_file(ts_dict, metric_id, start_time, end_time,
                       metric_rollup):
    """
    Write the time series data into specific files

    :param ts_dict: Time series information from configuration file.
    :param metric_id: Metric ID
    :param start_time: Start second time
    :param end_time: End second time
    :param metric_rollup: rollup
    """

    # Get the time series data from server
    time_series_data = pull_ts_data_from_server(ts_dict['ts_server'],
                                                metric_id,
                                                start_time * 1000,
                                                end_time * 1000,
                                                metric_rollup)

    # write each time series data into specific file
    if len(time_series_data.data.values()) > 0:
        time_series_file_name = ''
        time_series_file = None
        for single_data in time_series_data.data.values()[0].timeValues:
            # Get second time from millisecond time
            time_stamp = single_data.timestampMs / 1000
            # Set metric Id for each data
            single_data.metric_id = metric_id

            # Get specific time series file
            new_file = get_time_series_file_path(int(time_stamp),
                                                 ts_dict['interval'],
                                                 ts_dict['time_range'],
                                                 ts_dict['ts_directory'],
                                                 'data')
            if new_file != time_series_file_name:
                if time_series_file is not None:
                    time_series_file.close()
                time_series_file_name = new_file
                time_series_file = open(time_series_file_name, 'a')

            # Append data into file
            value = single_data.value.doubleValue
            line = '{timeStamp},{metric_id},{value}\n'.format(
                timeStamp=str(single_data.timestampMs / 1000),
                metric_id=metric_id,
                value=str(value)
            )
            time_series_file.write(line)
        time_series_file.close()


def download_single_ts_data(ts_dict, metric_id, start, end, metric_rollup):
    """
    Assign write data task. Pull the data from the whole time range first. If
    the time series data is huge, the ts server will raise an exception. This
    system will split this task into 2 small tasks and do them recursively.


    :param ts_dict: Time series information from configuration file.
    :param metric_id: Metric ID
    :param start: Start second time
    :param end: End second time
    """
    try:
        write_ts_data_file(ts_dict, metric_id, start, end, metric_rollup)
    except TsdbException:
        download_single_ts_data(ts_dict,
                                metric_id,
                                start,
                                int((start + end) / 2),
                                metric_rollup)
        download_single_ts_data(ts_dict,
                                metric_id,
                                int((start + end) / 2),
                                end,
                                metric_rollup)


def convert_time_series_data(input_file, output_file, time_range):
    """
    Convert the time series data file into a json file grouped by timestamp.

    :param input_file: The input data file with time series data
    :param output_file: The output json file
    :param time_range: The time range
    """
    # Open the raw time series data file
    with open(input_file) as raw_file:
        raw_data = raw_file.read()
    tsdata = {}

    def emit(line):
        """
        Put all time series data into a map

        :param line: raw ts data
        """
        array = line.split(',')
        second_shift = get_second_shift(int(array[0]), time_range)
        new_value = {'id': array[1], 'value': float(array[2])}
        if second_shift in tsdata:
            tsdata[second_shift]['data'].append(new_value)
        else:
            tsdata[second_shift] = {'old_time': array[0], 'data': []}
            tsdata[second_shift]['data'] = [new_value]

    # Handle all raw time series data by line
    result_array = raw_data.split('\n')
    map(emit, result_array[:len(result_array) - 1])

    # Write this map into a json file
    with open(output_file, 'w') as outfile:
        json.dump(tsdata, outfile, indent=4)


def convert_all_time_series_data(ts_dict):
    """
    Convert all raw time series data to json data

    :param folder_path: The folder path of all time series data
    :param time_range: The time range
    """
    # Get all raw time series data file
    files = glob.glob(ts_dict['ts_directory'] + "/*.data")
    for file_path in files:
        new_file_path = file_path[:-5] + ".json"
        # Convert into json file
        convert_time_series_data(file_path, new_file_path,
                                 ts_dict['time_range'])
        os.remove(file_path)


def record_by_config(record_dict, config_file):
    # Create data directory
    create_folder_path(record_dict['data_directory'])
    create_folder_path(record_dict['ts_directory'])

    shutil.copy(config_file, record_dict['record_config'])
    metadata = get_metadata(record_dict['api_server'],
                            record_dict['query'],
                            record_dict['record_token'])

    number = 0
    for metric_id in metadata.keys():
        number += 1
        try:
            download_single_ts_data(record_dict, str(metric_id),
                                    record_dict['start'],
                                    record_dict['end'],
                                    metadata[metric_id]['sf_metricType'])
            print "Record data from {metric_id}, {number}/{total}".format(
                metric_id=metric_id,
                number=number,
                total=len(metadata.keys())
            )
        except Exception:
            pass

    # Write metadata into file
    with open(record_dict['metadata_path'], 'w') as outfile:
        json.dump(metadata, outfile, indent=4)

    # Convert raw time series data to json file
    convert_all_time_series_data(record_dict)

    # Make tarball
    if os.path.isfile(TAR_NAME):
        os.remove(TAR_NAME)

    with tarfile.open(TAR_NAME, "w:gz") as tar:
        tar.add(record_dict['data_directory'], arcname='data')


def record_data(config_file):
    """
    Record metadata and time series data into files

    :param config_file: Configuration file for record data.
    """
    # Open the json configuration file
    config = read_record_config(config_file)
    record_dict = check_record_config(config)
    record_by_config(record_dict, config_file)
