#!/usr/bin/env python
"""
This file implements all functions about publishing data.
"""
import signalfx
import json
import logging
import os
import time

from time import sleep
from src.util import TS_DATA_DIR
from src.util import METADATA_FILE
from src.util import CONFIG_FILE
from src.util import TIME_INFOR
from src.util import get_new_interval_information
from src.util import get_second_shift
from src.util import get_next_time_series_file_path
from src.util import check_data_dir
from src.util import read_record_config
from src.util import check_record_config
from src.util import get_time_series_file_path


def send_signal_time_data(data, metadata, client, verbose):
    """
    Send data in one timestamp

    :param data: Time seies data need to publish
    :param metadata: Metadata need to construct the new time series data
    :param client: Signalfx client to publish data
    """

    gauges_metrics = []
    counter_metrics = []
    cumulative_counter_metrics = []

    def construct_single_data(single_data):
        """
        Construct a data item to with time series data value and meta data

        :param single_data: raw time series data
        """
        metric_id = str(single_data['id'])
        time_stamp = time.time() * 1000
        metric = {
            'metric': str(metadata[metric_id]['sf_metric']),
            'value': single_data['value'],
            'timestamp': time_stamp,
            'dimensions': metadata[metric_id]['dimensions']
        }
        if metadata[metric_id]['sf_metricType'] == 'GAUGE':
            gauges_metrics.append(metric)
        elif metadata[metric_id]['sf_metricType'] == 'COUNTER':
            counter_metrics.append(metric)
        elif metadata[metric_id]['sf_metricType'] == 'CUMULATIVE_COUNTER':
            cumulative_counter_metrics.append(metric)

    # Construct all data items
    map(construct_single_data, data)
    if verbose:
        logging.info('gauges_metrics: {0}'.format(len(gauges_metrics)))
        logging.info('counter_metrics: {0}'.format(len(counter_metrics)))
        logging.info('cumulative_counter_metrics: {0}'.format(
            len(cumulative_counter_metrics)))
    else:
        logging.info('send {0} data'.format(len(gauges_metrics) +
                                            len(counter_metrics) +
                                            len(cumulative_counter_metrics)))

    # Publish all new time series data
    try:
        client.send(gauges=gauges_metrics, counters=counter_metrics,
                    cumulative_counters=cumulative_counter_metrics)
    except Exception as err:
        logging.error({"Send Data Error": err.message})


def publish_one_file_data(client, metadata, tsdata_file, publish_dict):
    """
    Publish all data from one file

    :param client: signalfx client
    :param metadata: metadata information
    :param publish_dict: publish dictionary
    :param tsdata_file: time series data file
    """
    # Open the time series data file and load ts data
    with open(tsdata_file) as tsdata_file:
        tsdata = json.load(tsdata_file)

        # Sort the time stamp
        time_series = map(int, tsdata.keys())
        time_series.sort()
        # Get new Information
        current_second_shift, next_index = get_new_interval_information(
            time_series, publish_dict['interval'], publish_dict['time_range'])

    while next_index < len(tsdata):
        sleep(time_series[next_index] - current_second_shift)
        logging.info("{current_time} ==> Current time.".format(
            current_time=time.ctime(time.time())))
        if publish_dict['verbose']:
            logging.info("{old_time} ==> Old time.".format(old_time=time.ctime(
                int(tsdata[str(time_series[next_index])]['old_time']))))
        # Send all data at this time stamp
        send_signal_time_data(tsdata[str(time_series[next_index])]['data'],
                              metadata, client, publish_dict['verbose'])
        next_index += 1
        current_second_shift = get_second_shift(time.time(),
                                                publish_dict['time_range'])


def publish_tsdata(publish_dict):
    """
    Publish the new time series data based on the old time series data
     and meta data.

    :param publish_dict: Publish information dictionary
    """

    # Load the meta data
    with open(publish_dict['metadata_path']) as metadata_file:
        metadata = json.load(metadata_file)
    # print(metadata)

    # Launch a client to send data to SignalFx
    client = signalfx.SignalFx(publish_dict['api_token'],
                               ingest_endpoint=publish_dict['ingest_endpoint'])

    # Get specific time series file
    tsdata_file = get_time_series_file_path(time.time(),
                                            publish_dict['interval'],
                                            publish_dict['time_range'],
                                            publish_dict['ts_directory'],
                                            'json')

    while True:
        if os.path.exists(tsdata_file):
            # Publish time series data of one file
            publish_one_file_data(client, metadata, tsdata_file, publish_dict)
        else:
            # If this file cannot exist, it is means no any data in this
            #  time slot, so sleep a time interval.
            sleep(TIME_INFOR[publish_dict['time_range']]['second_range'])

        # Get the next time series file
        tsdata_file = get_next_time_series_file_path(tsdata_file,
                                                     publish_dict['interval'],
                                                     publish_dict['time_range']
                                                     )


def publish_data(data_dir, api_token, ingest_endpoint, logfile, verbose):
    """
    Send the metric from json configuration file

    :param config_file: The configuration json file
    """
    # Open the json configuration file
    check_data_dir(data_dir)
    config = read_record_config(data_dir + '/' + CONFIG_FILE)
    publish_dict = check_record_config(config)
    publish_dict['api_token'] = api_token
    publish_dict['ingest_endpoint'] = ingest_endpoint
    publish_dict['ts_directory'] = data_dir + '/' + TS_DATA_DIR
    publish_dict['metadata_path'] = data_dir + '/' + METADATA_FILE
    publish_dict['verbose'] = verbose
    if logfile is not None:
        logging.basicConfig(filename=str(logfile), level=logging.INFO)

    print("Start sending data ...")
    publish_tsdata(publish_dict)
