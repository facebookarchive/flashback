"""Globally shared common utilities functions/classes/variables"""
import logging
import config
import cPickle
import time
import pymongo
import string
import threading
import constants


def _make_logger():
    """Create a new logger"""
    logger = logging.getLogger("parse.flashback")
    logger.setLevel(config.APP_CONFIG["logging_level"])
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s:%(processName)s] %(message)s",
        "%m-%d %H:%M:%S"))
    logger.addHandler(handler)

    return logger

# log will be used globally for all logging code.
LOG = _make_logger()


def unpickle(input_file):
    """Safely unpack entry from the file"""
    try:
        return cPickle.load(input_file)
    except EOFError:
        return None


def unpickle_iterator(filename):
    """Return the unpickled objects as a sequence of objects"""
    f = open(filename)
    while True:
        result = unpickle(f)
        if result:
            yield result
        else:
            raise StopIteration


def now_in_utc_secs():
    """Get current time in seconds since UTC epoch"""
    return int(time.time())


def create_tailing_cursor(collection, criteria, oplog=False):
    """
    Create a cursor that constantly tails the latest documents from the
    database.

    criteria is a query dict (for filtering op types, targeting a specifc set
    of collections, etc.).
    """
    tailer = collection.find(
        criteria, slave_okay=True, tailable=True, await_data=True)

    # Set oplog_replay on the cursor, which allows queries against the oplog to run much faster
    if oplog:
        tailer.add_option(pymongo.cursor._QUERY_OPTIONS['oplog_replay'])

    return tailer


def get_start_time(collection):
    """Get the latest element's timestamp from a collection with "ts" field"""
    result = collection.find().limit(1).sort([("ts", pymongo.DESCENDING)])
    try:
        return result.next()["ts"]
    except StopIteration:
        return None


class EmptyClass(object):

    """Empty class"""


def set_interval(interval, start_immediately=True, exec_on_exit=True):
    """An decorator that executes the event every n seconds"""
    def decorator(function):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():  # executed in another thread
                if start_immediately:
                    function(*args, **kwargs)
                while not stopped.wait(interval):  # until stopped
                    function(*args, **kwargs)
                if exec_on_exit:
                    function(*args, **kwargs)

            t = threading.Thread(target=loop)
            t.daemon = True  # stop if the program exits
            t.start()
            return stopped
        return wrapper
    return decorator


def make_ns_selector(databases, target_collections):
    system_collections = \
        set([constants.PROFILER_COLLECTION, constants.INDEX_COLLECTION])

    if target_collections is not None:
        target_collections = set(target_collections)
        target_collections -= system_collections

    if target_collections is not None and len(target_collections) > 0:
        return {"$in": ["{0}.{1}".format(database, coll)
                for coll in target_collections
                for database in databases]}
    else:
        return {
            "$regex": r"^({})\.".format(string.join(databases, '|')),
            "$nin": ["{0}.{1}".format(database, coll)
                        for coll in system_collections
                        for database in databases]
        }


def get_oplog_tailer(oplog_client, types, target_dbs, target_colls,
                     start_time=None):
    """Start recording the oplog entries starting from now.
    We only care about "insert" operations since all other queries will
    be captured by mongodb oplog collection.

    REQUIRED: the specific mongodb database has enabled profiling.
    """
    oplog_collection = \
        oplog_client[constants.LOCAL_DB][constants.OPLOG_COLLECTION]
    criteria = {
        "op": {"$in": types},
        "ns": make_ns_selector(target_dbs, target_colls)
    }

    if start_time is not None:
        criteria["ts"] = {"$gte": start_time}
    return create_tailing_cursor(oplog_collection, criteria, oplog=True)


def get_profiler_tailer(client, target_db, target_colls, start_time):
    """Start recording the profiler entries"""
    profiler_collection = client[target_db][constants.PROFILER_COLLECTION]
    criteria = {
        "ns": make_ns_selector([target_db], target_colls),
        "ts": {"$gte": start_time}
    }

    return create_tailing_cursor(profiler_collection, criteria)


class DictionaryCopier(object):

    """Simple tool for copy the fields from source dict on demand"""

    def __init__(self, source):
        self.src = source
        self.dest = {}

    def copy_fields(self, *fields):
        for field in fields:
            if field in self.src:
                self.dest[field] = self.src[field]
