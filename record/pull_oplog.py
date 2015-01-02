"""TODO(kailiu): this script is not fully completed. Sometimes we only care
about the results from oplog. This script will direct pull everything directly
from oplog."""
import sys
import utils
import config
import time
from datetime import datetime
from pymongo import MongoClient
from bson.json_util import dumps


def sanitize_op(op):
    new_op = {}
    new_op["ts"] = op["ts"].as_datetime()
    new_op["ns"] = op["ns"]
    op_type = op["op"]

    # handpick some essential fields to execute.
    if op_type == "i":
        new_op["op"] = "insert"
        new_op["o"] = op["o"]
    elif op_type == "u":
        new_op["op"] = "update"
        new_op["updateobj"] = op["o"]
        new_op["query"] = op["o2"]
    else:
        assert "Cannot recognize the op type: " + op_type

    return new_op


def dump_op(op, output_file):
    output_file.write(dumps(op))
    output_file.write("\n")


def write_to_file(tailer, duration_in_sec, output_file, check_duration_secs=1):
    start_ts = None
    start_ts_datetime = None
    entries_received = 0
    real_start = datetime.now()
    while tailer.alive:
        try:
            op = tailer.next()
            last_received_ts = op["ts"]
            if start_ts is None:
                utils.LOG.info("Start to pulling oplog, first entry ts: %s",
                               str(last_received_ts.as_datetime()))
                start_ts = last_received_ts
                start_ts_datetime = last_received_ts.as_datetime()

            if last_received_ts.time >= start_ts.time + duration_in_sec:
                break

            op = sanitize_op(op)
            dump_op(op, output_file)
            entries_received += 1
            if entries_received % 10000 == 0:
                utils.LOG.info(
                    "entries received %d, time advanced in oplog %s, time "
                    "advanced in real life %s",
                    entries_received, str(op["ts"] - start_ts_datetime),
                    str(datetime.now() - real_start))
        except StopIteration:
            time.sleep(check_duration_secs)

    utils.LOG.info("collected %d entries", entries_received)


def main():
    db_config = config.DB_CONFIG
    duration_in_sec = int(sys.argv[1])
    output = open(sys.argv[2], "w")

    # Get the tailer for oplog
    mongodb_uri = db_config["oplog_server"]["mongodb_uri"]
    oplog_client = MongoClient(mongodb_uri)
    tailer = utils.get_oplog_tailer(oplog_client,
                                    ["i", "u"],
                                    db_config["target_database"],
                                    db_config["target_collections"])

    # sanitize
    write_to_file(tailer, duration_in_sec, output)


if __name__ == '__main__':
    main()
