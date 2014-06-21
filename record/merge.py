"""This script allows us to manually merge the results from oplog and profiling
results."""
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


def merge_to_final_output(oplog_output_file, profiler_output_file, output_file):
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
    profiler = open(profiler_output_file, "rb")
    output = open(output_file, "wb")
    logger = utils.LOG

    logger.info("Starts completing the insert options")
    oplog_doc = utils.unpickle(oplog)
    profiler_doc = utils.unpickle(profiler)
    inserts = 0
    noninserts = 0
    severe_inconsistencies = 0
    mild_inconsistencies = 0

    while oplog_doc and profiler_doc:
        if (noninserts + inserts) % 2500 == 0:
            logger.info("processed %d items", noninserts + inserts)

        if profiler_doc["op"] != "insert":
            dump_op(output, profiler_doc)
            noninserts += 1
            profiler_doc = utils.unpickle(profiler)
        else:
            # Replace the the profiler's insert operation doc with oplog's,
            # but keeping the canonical form of "ts".
            profiler_ts = calendar.timegm(profiler_doc["ts"].timetuple())
            oplog_ts = oplog_doc["ts"].time
            # only care about the second-level precision.
            # This is a lame enforcement of consistency
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

            oplog_doc["ts"] = profiler_doc["ts"]
            # make sure "op" is "insert" instead of "i".
            oplog_doc["op"] = profiler_doc["op"]
            dump_op(output, oplog_doc)
            inserts += 1
            oplog_doc = utils.unpickle(oplog)
            profiler_doc = utils.unpickle(profiler)

    while profiler_doc and profiler_doc["op"] != "insert":
        dump_op(output, profiler_doc)
        noninserts += 1
        profiler_doc = utils.unpickle(profiler)

    logger.info("Finished completing the insert options, %d inserts and"
                " %d noninserts\n"
                "  severe ts incosistencies: %d\n"
                "  mild ts incosistencies: %d\n", inserts, noninserts,
                severe_inconsistencies, mild_inconsistencies)
    for f in [oplog, profiler, output]:
        f.close()

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
