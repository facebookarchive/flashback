""" This file serves as an example of config file. Please copy it to config.py.
"""
import logging

DB_CONFIG = {
    # Indicates which database to record.
    "target_database": "foo",
    # Indicates which collections to record. If user wants to capture all the
    # collections' activities, leave this field to be `None` (but we'll always
    # skip collection `system.profile`, even if it has been explicit
    # specified).
    "target_collections": [],
    "oplog_server": {
        "host": "localhost",
        "port": 30000,
    },
    # In most cases you will record from the profile DB on the primary
    # If you are also sending queries to secondaries, you may want to specify
    # a list of secondary servers in addition to the primary
    "profiler_servers": [
        {
            "host": "localhost",
            "port": 30000,
        },
        {
            "host": "localhost",
            "port": 30001,
        },
        {
            "host": "localhost",
            "port": 30002,
        }
    ],
    "oplog_output_file": "./OPLOG_OUTPUT",
    "profiler_output_file": "./PROFILER_OUTPUT",
    "output_file": "./OUTPUT",
    # the length for the recording
    "duration_secs": 5
}

APP_CONFIG = {
    "logging_level": logging.DEBUG
}
