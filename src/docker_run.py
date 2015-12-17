#!/usr/bin/env python
"""
This file is used by docker-based publish tool.

- Collect environment variables.
- Publish ts data based on configuration file.
"""
import os
from src.publish_data import publish_data
from src.util import Error

DOCKER_DATA_DIR = '/opt/data'


def get_environ_variable(key):
    if key not in os.environ.keys():
        raise Error('Do not have environment variable [{0}]'.format(key))
    return os.environ[key]


if __name__ == '__main__':
    # open configuration file
    try:
        api_token = get_environ_variable('api_token')
        ingest_endpoint = get_environ_variable('ingest_endpoint')
        logfile = os.environ.get('log_file', None)
        verbose = 'verbose' in os.environ.keys() and \
                  os.environ['verbose'] == 'true'
        publish_data(DOCKER_DATA_DIR, api_token, ingest_endpoint, logfile,
                     verbose)
    except Error as e:
        print e.message
