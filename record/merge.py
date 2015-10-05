"""This script allows us to manually merge the results from oplog and profiling
results."""
import os
import utils
import config
import calendar
import sys
from bson.json_util import dumps


def dump_op(output, op):
    copier = utils.DictionaryCopier(op)
    copier.copy_fields("ts", "ns", "op")
    op_type = op["op"]

    # handpick some essential fields to execute.
    if op_type == "query":
        copier.copy_fields("query", "ntoskip", "ntoreturn")
    elif op_type == "insert":
        copier.copy_fields("o")
    elif op_type == "update":
        copier.copy_fields("updateobj", "query")
    elif op_type == "remove":
        copier.copy_fields("query")
    elif op_type == "command":
        copier.copy_fields("command")

    output.write(dumps(copier.dest))
    output.write("\n")


def merge_to_final_output(oplog_output_file, profiler_output_files, output_file):
    """
    * Why merge files:
        we need to merge the docs from two sources into one.
    * Why not merge earlier:
        It's definitely inefficient to merge the entries when we just retrieve
        these documents from mongodb. However we designed this script to be able
        to pull the docs from differnt servers, as a result it's hard to do the
        on-time merge since you cannot determine if some "old" entries will come
        later."""
    oplog = open(oplog_output_file, "rb")

    # create a map of profiler file names to files
    profiler_files = {}
    for profiler_file in profiler_output_files:
        profiler_files[profiler_file] = open(profiler_file, "rb")

    output = open(output_file, "wb")
    logger = utils.LOG

    logger.info("Starts completing the insert options")
    oplog_doc = utils.unpickle(oplog)

    # Create a map of tuple(doc's timestamp, profiler file name) to doc for
    # each profiler. This makes it easy to fetch the earliest doc in the group
    # on each iteration.
    profiler_docs = {}
    for file_name in profiler_files:
        doc = utils.unpickle(profiler_files[file_name])
        # associate doc with a tuple representing the ts and source filename
        # this makes it easy to fetch the earliest doc in the group on each
        # iteration
        if doc:
            profiler_docs[(doc["ts"], file_name)] = doc
    inserts = 0
    noninserts = 0
    severe_inconsistencies = 0
    mild_inconsistencies = 0

    # read docs until either we exhaust the oplog or all ops in the profile logs
    while oplog_doc and len(profiler_docs) > 0:
        if (noninserts + inserts) % 2500 == 0:
            logger.info("processed %d items", noninserts + inserts)

        # get the earliest profile doc out of all profiler_docs
        key = min(profiler_docs.keys())
        profiler_doc = profiler_docs[key]
        # remove the doc and fetch a new one
        del(profiler_docs[key])
        # the first field in the key is the file name
        doc = utils.unpickle(profiler_files[key[1]])
        if doc:
            profiler_docs[(doc["ts"], key[1])] = doc

        # If the retrieved operation is not an insert, we can simply dump it
        # to the output file. Otherwise, we need to cross-reference the
        # profiler's insert operation with an oplog entry (because the
        # profiler doesn't contain the inserted object's details).
        if profiler_doc["op"] != "insert":
            dump_op(output, profiler_doc)
            noninserts += 1
        else:
            # Compare the profile doc's ts with the oplog doc's ts. In the
            # ideal scenario, every insert we capture via the profile
            # collection should match a consecutive oplog entry (the oplog
            # tailer only looks at insert ops).
            profiler_ts = calendar.timegm(profiler_doc["ts"].timetuple())
            oplog_ts = oplog_doc["ts"].time
            delta = abs(profiler_ts - oplog_ts)
            if delta > 3:
                # TODO strictly speaking, this ain't good since the files are
                # not propertly closed.
                logger.error(
                    "oplog and profiler results are inconsistent `ts`\n"
                    "  oplog:    %d\n"
                    "  profiler: %d", oplog_ts, profiler_ts)
                severe_inconsistencies += 1
            elif delta != 0:
                logger.warn("Slightly inconsistent timestamp\n"
                            "  oplog:   %d\n"
                            "  profiler %d", oplog_ts, profiler_ts)
                mild_inconsistencies += 1

            oplog_doc["ts"] = profiler_doc["ts"]  # we still want to keep the canonical form of the ts
            oplog_doc["op"] = profiler_doc["op"]  # make sure "op" is "insert" instead of "i"
            dump_op(output, oplog_doc)
            inserts += 1

            # Get the next doc from the oplog
            oplog_doc = utils.unpickle(oplog)

    # finish up any remaining non-insert ops
    while len(profiler_docs) > 0:
        # get the earliest profile doc out of all profiler_docs
        key = min(profiler_docs.keys())
        profiler_doc = profiler_docs[key]
        # remove the doc and fetch a new one
        del(profiler_docs[key])
        doc = utils.unpickle(profiler_files[key[1]])
        if doc:
            profiler_docs[(doc["ts"], key[1])] = doc

        if profiler_doc["op"] == "insert":
            break
        dump_op(output, profiler_doc)
        noninserts += 1

    logger.info("Finished completing the insert options, %d inserts and"
                " %d noninserts\n"
                "  severe ts incosistencies: %d\n"
                "  mild ts incosistencies: %d\n", inserts, noninserts,
                severe_inconsistencies, mild_inconsistencies)

    for f in [oplog, output]:
        f.close()
    for f in profiler_files.values():
        f.close()

    # Clean up temporary files (oplog + profiler files), since everything is
    # already in the main output file
    for f in profiler_output_files:
        os.remove(f)
    os.remove(oplog_output_file)

    return True


def main():
    # TODO: this command is not user-friendly and doesn't do any sanity check
    # for the parameters.
    db_config = config.DB_CONFIG
    if len(sys.argv) != 1:
        params = sys.argv[1:]
        merge_to_final_output(params[0], params[1], params[2])
    else:
        merge_to_final_output(db_config["oplog_output_file"],
                              db_config["profiler_output_file"],
                              db_config["output_file"])

if __name__ == '__main__':
    main()
