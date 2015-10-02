#!/usr/bin/python
r""" Track the MongoDB activities by tailing oplog and profiler output"""

from bson.timestamp import Timestamp
from datetime import datetime
from pymongo import MongoClient, uri_parser
import pymongo
from threading import Thread
import config
import cPickle
import Queue
import time
import utils
import signal
import merge
import os
import sys


def tail_to_queue(tailer, identifier, doc_queue, state, end_time,
                  check_duration_secs=1):
    """
    Accept a tailing cursor and serialize the retrieved documents to a fifo
    queue.

    @param identifier: when passing the retrieved document to the queue, we
        will attach a unique identifier that allows the queue consumers to
        process different sources of documents accordingly. There's a separate
        identifier for the oplog and for each database-host pair for which
        we tail the profile collection.
    @param check_duration_secs: if we cannot retrieve the latest document,
        this queuing loop will sleep for that many seconds and then try again.
    """
    tailer_state = state.tailer_states[identifier]
    first_loop = True
    while all(s.alive for s in state.tailer_states.values()):
        try:
            # Get the next doc from the tailing cursor
            doc = tailer.next()
        except StopIteration:

            # If the tailing cursor is dead, print a debug message and break
            # out of the loop (since it's a tailing cursor, it should be alive
            # even if it can't return any new docs). One reason for a cursor
            # to die is if it becomes "stale", as in, the latest doc it
            # pointed to is no longer available in the collection.
            if not tailer.alive:
                utils.LOG.debug(
                    "%s: The tailing cursor is dead. Your profile collection "
                    "might be too small for the amount of new docs that are "
                    "being written to it.", identifier)
                break

            # If it's time to stop recording, break out of the loop
            if state.timeout:
                break

            tailer_state.last_get_none_ts = datetime.utcnow()
            time.sleep(check_duration_secs)
        except pymongo.errors.OperationFailure, e:
            utils.LOG.debug("%s", e)
            if first_loop:
                utils.LOG.error(
                    "BADRUN: %s: We appear to not have the %s collection created or is non-capped! %s",
                    identifier, tailer.collection, e)
        else:
            # Set last_received_ts based on the latest doc
            tailer_state.last_received_ts = doc["ts"]

            # If it's time to stop recording and the timestamp of the last
            # received doc is >= end_time, break out of the loop
            if state.timeout and tailer_state.last_received_ts >= end_time:
                break

            doc_queue.put_nowait((identifier, doc))
            tailer_state.entries_received += 1

        first_loop = False

    tailer_state.alive = False
    utils.LOG.info("%s: Tailing to queue completed!", identifier)


class MongoQueryRecorder(object):
    """Record MongoDB database's activities by polling the oplog and profiler
    results"""

    class RecordingState(object):
        """Keeps the running status of a recording request"""

        @staticmethod
        def make_tailer_state():
            """Return the tailer state "struct" """
            s = utils.EmptyClass()
            s.entries_received = 0  # how many entries were put in the global doc queue
            s.entries_written = 0  # how many entries were written to the output file
            s.alive = True  # is this thread still alive (i.e. working)?
            s.last_received_ts = None  # timestamp of the latest received doc
            s.last_get_none_ts = None  # last time this thread didn't get anything from the tailing cursor
            return s

        def __init__(self, tailer_names):
            self.timeout = False
            self.tailer_states = {}
            for name in tailer_names:
                self.tailer_states[name] = self.make_tailer_state()

    def __init__(self, db_config):
        self.config = db_config
        self.force_quit = False

        # Sanitize the config options
        self.config["target_collections"] = set(
            [coll.strip() for coll in self.config.get("target_collections", [])])
        if self.config.get('auto_config'):
            if 'auto_config_options' not in self.config:
                self.config['auto_config_options'] = {}
            if 'auth_db' not in self.config['auto_config_options'] and 'auth_db' in self.config:
                self.config['auto_config_options']['auth_db'] = self.config['auth_db']
            if 'user' not in self.config['auto_config_options'] and 'user' in self.config:
                self.config['auto_config_options']['user'] = self.config['user']
            if 'password' not in self.config['auto_config_options'] and 'password' in self.config:
                self.config['auto_config_options']['password'] = self.config['password']

            self.get_topology(self.config['auto_config_options'])
            oplog_servers = self.build_oplog_servers(self.config['auto_config_options'])
            profiler_servers = self.build_profiler_servers(self.config['auto_config_options'])
        else:
            oplog_servers = self.config["oplog_servers"]
            profiler_servers = self.config["profiler_servers"]

        if len(oplog_servers) < 1 or len(profiler_servers) < 1:
            utils.log.error("Detected either no profile or oplog servers, bailing")
            sys.exit(1)

        # Connect to each MongoDB server that we want to get the oplog data from
        self.oplog_clients = {}
        for index, server in enumerate(oplog_servers):
            mongodb_uri = server['mongodb_uri']
            nodelist = uri_parser.parse_uri(mongodb_uri)["nodelist"]
            server_string = "%s:%s" % (nodelist[0][0], nodelist[0][1])

            self.oplog_clients[server_string] = self.connect_mongo(server)
            utils.LOG.info("oplog server %d: %s", index, self.sanitize_server(server))

        # Connect to each MongoDB server that we want to get the profile data from
        self.profiler_clients = {}
        for index, server in enumerate(profiler_servers):
            mongodb_uri = server['mongodb_uri']
            nodelist = uri_parser.parse_uri(mongodb_uri)["nodelist"]
            server_string = "%s:%s" % (nodelist[0][0], nodelist[0][1])

            self.profiler_clients[server_string] = self.connect_mongo(server)
            utils.LOG.info("profiling server %d: %s", index, self.sanitize_server(server))

        utils.LOG.info('Successfully connected to %d oplog server(s) and %d profiler server(s)', len(self.oplog_clients), len(self.profiler_clients))

    def sanitize_server(self, server_config):
        if 'user' in server_config:
            server_config['user'] = "Redacted"
        if 'password' in server_config:
            server_config['password'] = "Redacted"
        return server_config

    @staticmethod
    def _process_doc_queue(doc_queue, files, state):
        """Writes the incoming docs to the corresponding files"""

        # Keep receiving docs via the global doc queue and dumping them to
        # files if any of the tailer threads are still at work or if the
        # doc queue is not empty.
        queue_is_empty = False
        while any(s.alive for s in state.tailer_states.values()) or not queue_is_empty:
            try:
                name, doc = doc_queue.get(block=True, timeout=1)
            except Queue.Empty:
                # Continue if we still can't get anything from the queue after the timeout
                queue_is_empty = True
                continue
            else:
                queue_is_empty = False
                state.tailer_states[name].entries_written += 1
                cPickle.dump(doc, files[name])

        for f in files.values():
            f.flush()

        utils.LOG.info("All received docs are processed!")

    @staticmethod
    def _report_status(state):
        """report current processing status"""
        msgs = []
        for key in state.tailer_states.keys():
            tailer_state = state.tailer_states[key]

            # oplog tailer returns "ts" as a Timestamp - we want to be able
            # to easily compare last_received_ts and last_get_none_ts, so
            # we convert the Timestamp to a datetime
            last_received_ts = tailer_state.last_received_ts
            if isinstance(last_received_ts, Timestamp):
                last_received_ts = last_received_ts.as_datetime()

            msg = "\n\t{}: received {} entries, {} of them were written, "\
                  "last received entry ts: {}, last get-none ts: {}" .format(
                      key,
                      tailer_state.entries_received,
                      tailer_state.entries_written,
                      str(last_received_ts),
                      str(tailer_state.last_get_none_ts))
            msgs.append(msg)

        utils.LOG.info("".join(msgs))

    def get_topology(self, config_options):
        topology = {}
        mongos_conn = self.connect_mongo(config_options)
        temp_topology = mongos_conn.admin.command("connPoolStats")
        if 'replicaSets' in temp_topology:
            for shard in temp_topology['replicaSets']:
                topology[shard] = {'primary': None, 'secondaries': []}
                for host in temp_topology['replicaSets'][shard]['hosts']:
                    if host['ismaster'] is True:
                        topology[shard]['primary'] = host['addr']
                    elif host['secondary'] is True:
                        topology[shard]['secondaries'].append(host['addr'])
        else:
            return False

        self.topology = topology
        return True

    def build_oplog_servers(self, config_options):
        oplog_servers = []
        for shard in self.topology:
            temp_server = {
                'mongodb_uri': "mongodb://%s" % self.topology[shard]['primary'],
                'replSet':  shard,
                'auth_db':  config_options['auth_db'],
                'user':     config_options['user'],
                'password': config_options['password']
            }
            oplog_servers.append(temp_server)
        return oplog_servers

    def build_profiler_servers(self, config_options):
        profiler_servers = []
        for shard in self.topology:
            temp_server = {
                'mongodb_uri': "mongodb://%s" % self.topology[shard]['primary'],
                'replSet':  shard,
                'auth_db':  config_options['auth_db'],
                'user':     config_options['user'],
                'password': config_options['password']
            }
            profiler_servers.append(temp_server)
            if self.config['auto_config'] is True:
                if 'use_secondaries' in self.config['auto_config_options']:
                    if self.config['auto_config_options']['use_secondaries'] is True:
                        for node in self.topology[shard]['secondaries']:
                            temp_server = {
                                'mongodb_uri': "mongodb://%s" % node,
                                'auth_db':  config_options['auth_db'],
                                'user':     config_options['user'],
                                'password': config_options['password']
                            }
                            profiler_servers.append(temp_server)
        return profiler_servers

    def connect_mongo(self, server_config):
        """Connect to MongoDB and return an initialized MongoClient"""
        if 'replSet' not in server_config:
            client = MongoClient(server_config['mongodb_uri'], slaveOk=True)
        else:
            client = MongoClient(server_config['mongodb_uri'], slaveOk=True, replicaset=server_config['replSet'])

        if server_config.get('auth_db') is not None \
           and server_config.get('user') is not None \
           and server_config.get('password') is not None:
                try:
                    client[server_config['auth_db']].authenticate(
                        server_config['user'], server_config['password'])
                except Exception, e:
                    utils.log.error("Unable to authenticated to %s: %s " %
                                    (server_config['mongodb_uri'], e))
                    sys.exit(1)
        return client

    def force_quit_all(self):
        """Gracefully quit all recording activities"""
        self.force_quit = True

    def _generate_workers(self, files, state, start_utc_secs, end_utc_secs):
        """Generate the threads that tail the data sources and put the fetched
        entries to the files"""

        # Initialize a thread-safe queue that we'll put the docs into
        doc_queue = Queue.Queue()

        # Initialize a list that will keep track of all the worker threads that
        # handle tracking/dumping of mongodb activities
        workers_info = []

        # Writer thread, we only have one writer since we assume all files will
        # be written to the same device (disk or SSD), as a result it yields
        # not much benefit to have multiple writers.
        workers_info.append({
            "name": "write-all-docs-to-file",
            "thread": Thread(
                target=MongoQueryRecorder._process_doc_queue,
                args=(doc_queue, files, state))
        })

        # For each server in the "oplog_servers" config...
        for server_string, mongo_client in self.oplog_clients.items():

            # Create a tailing cursor (aka a tailer) on an oplog collection
            tailer = utils.get_oplog_tailer(mongo_client, ["i"],
                                            self.config["target_databases"],
                                            self.config["target_collections"],
                                            Timestamp(start_utc_secs, 0))

            # Create a new thread and add some metadata to it
            workers_info.append({
                "name": "tailing-oplogs on %s" % server_string,
                "on_close":
                    lambda: self.oplog_client.kill_cursors([tailer.cursor_id]),
                "thread": Thread(
                    target=tail_to_queue,
                    args=(tailer, "oplog", doc_queue, state,
                          Timestamp(end_utc_secs, 0)))
            })

        start_datetime = datetime.utcfromtimestamp(start_utc_secs)
        end_datetime = datetime.utcfromtimestamp(end_utc_secs)


        # For each server in the "profiler_servers" config...
        for server_string, mongo_client in self.profiler_clients.items():

            # For each database in the "target_databases" config...
            for db in self.config["target_databases"]:

                # Create a tailing cursor (aka a tailer) on a profile collection
                tailer = utils.get_profiler_tailer(mongo_client, db,
                                                   self.config["target_collections"],
                                                   start_datetime)

                # Create a new thread and add some metadata to it
                tailer_id = "%s_%s" % (db, server_string)
                workers_info.append({
                    "name": "tailing-profiler for %s on %s" % (db, server_string),
                    "on_close":
                        lambda: self.profiler_client.kill_cursors([tailer.cursor_id]),
                    "thread": Thread(
                        target=tail_to_queue,
                        args=(tailer, tailer_id, doc_queue, state,
                              end_datetime))
                })

        # Deamonize each thread and start it
        for worker_info in workers_info:
            utils.LOG.info("Starting thread: %s", worker_info["name"])
            worker_info["thread"].setDaemon(True)
            worker_info["thread"].start()

        # Return the list of all the started threads
        return workers_info

    def _join_workers(self, state, workers_info):
        """Prepare to exit all workers"""
        for idx, worker_info in enumerate(workers_info):
            utils.LOG.info(
                "Time to stop, waiting for thread: %s to finish",
                worker_info["name"])
            thread = worker_info["thread"]
            name = worker_info["name"]
            # Idempotently wait for thread to exit
            wait_secs = 5
            while thread.is_alive():
                thread.join(wait_secs)
                if thread.is_alive():
                    if self.force_quit and "on_close" in worker_info:
                        worker_info["on_close"]()
                    utils.LOG.error(
                        "Thread %s didn't exit after %d seconds. Will wait for "
                        "another %d seconds", name, wait_secs, 2 * wait_secs)
                    wait_secs *= 2
                    thread.join(wait_secs)
                else:
                    utils.LOG.info("Thread %s exits normally.", name)

    @utils.set_interval(3)
    def _periodically_report_status(self, state):
        return MongoQueryRecorder._report_status(state)

    def record(self):
        """Record the activities in a multithreaded way"""
        start_utc_secs = utils.now_in_utc_secs()
        end_utc_secs = utils.now_in_utc_secs() + self.config["duration_secs"]

        # If overwrite_output_file setting is False, determine the actual name
        # of the output file
        if not self.config["overwrite_output_file"]:
            cnt = 2
            while os.path.exists(self.config["output_file"]):
                self.config["oplog_output_file"] += '_%d' % cnt
                self.config["output_file"] += '_%d' % cnt

        # We'll dump the recorded activities to `files`.
        files = {
            "oplog": open(self.config["oplog_output_file"], "wb")
        }
        tailer_names = []
        profiler_output_files = []

        # Open a file for each profiler client, append client name as suffix
        for client_name in self.profiler_clients:
            # create a file for each (client,db)
            for db in self.config["target_databases"]:
                tailer_name = "%s_%s" % (db, client_name)
                tailer_names.append(tailer_name)
                profiler_output_files.append(tailer_name)
                files[tailer_name] = open(tailer_name, "wb")
        tailer_names.append("oplog")

        state = MongoQueryRecorder.RecordingState(tailer_names)

        # Create the working threads that handle tracking and dumping of
        # mongodb activities. On return, these threads will have already
        # started.
        workers_info = self._generate_workers(files, state, start_utc_secs,
                                              end_utc_secs)
        timer_control = self._periodically_report_status(state)

        # Waiting till due time arrives
        while all(s.alive for s in state.tailer_states.values()) \
                and (utils.now_in_utc_secs() < end_utc_secs) \
                and not self.force_quit:
            time.sleep(1)

        # Log the reason for stopping
        utils.LOG.debug("Stopping the recording! All tailers alive: %s; End time passed: %s; Force quit requested: %s.",
            all(s.alive for s in state.tailer_states.values()),
            (utils.now_in_utc_secs() >= end_utc_secs),
            self.force_quit
        )

        # Indicate that it's time to stop
        state.timeout = True

        # Wait until all the workers finish
        self._join_workers(state, workers_info)

        # Stop reporting the status
        timer_control.set()
        utils.LOG.info("Preliminary recording completed!")

        # Close all the file handlers
        for f in files.values():
            f.close()

        # Fill the missing insert op details from the oplog
        merge.merge_to_final_output(
            oplog_output_file=self.config["oplog_output_file"],
            profiler_output_files=profiler_output_files,
            output_file=self.config["output_file"])


def main():
    """Recording the inbound traffic for a database."""
    db_config = config.DB_CONFIG
    recorder = MongoQueryRecorder(db_config)

    def signal_handler(sig, dummy):
        """Handle the Ctrl+C signal"""
        print 'Trying to gracefully exit the program...'
        recorder.force_quit_all()
    signal.signal(signal.SIGINT, signal_handler)

    recorder.record()

if __name__ == '__main__':
    main()
