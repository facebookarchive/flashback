""" This file serves as an example of config file. Please copy it to config.py.
"""
import logging

DB_CONFIG = {
    # Indicates which database to record.
    "target_databases": ["foo", "bar"],
    # Indicates which collections to record. If user wants to capture all the
    # collections' activities, leave this field to be `None` (but we'll always
    # skip collection `system.profile`, even if it has been explicit
    # specified).
    "target_collections": [],
    "oplog_servers": [
        { "mongodb_uri": "mongodb://localhost:30000" },
    ],
    # In most cases you will record from the profile DB on the primary
    # If you are also sending queries to secondaries, you may want to specify
    # a list of secondary servers in addition to the primary
    "profiler_servers": [
        { "mongodb_uri": "mongodb://localhost:30000" },
        { "mongodb_uri": "mongodb://localhost:30001" },
        { "mongodb_uri": "mongodb://localhost:30002" },
    ],
    "oplog_output_file": "./OPLOG_OUTPUT",
    "output_file": "./OUTPUT",

    # If overwrite_output_file is True, the same output file will be
    # overwritten is False in between consecutive calls of the recorer. If
    # it's False, the recorder will append a unique number to the end of the
    # output_file if the original one already exists.
    "overwrite_output_file": True,

    # the length for the recording
    "duration_secs": 5
}

APP_CONFIG = {
    "logging_level": logging.DEBUG
}
