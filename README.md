# What is Flashback

How can you measure how good your MongoDB (or other databases with similar interface) performance is? Easy, you can benchmark it. A general way to solve this problem is to use a benchmark tool to generate queries with random contents under certain random distribution.

But sometimes you are not satisfied with the randomly generated queries, since you're not confident in how much these queries resemble your real workload.

The difficulty compounds when one MongoDB instance may host completely different types of databases that each have their own unique and complicated access patterns.

That is the reason we came up with `Flashback`, a MongoDB benchmark framework that allows us to benchmark with "real" queries. It is comprised of a set of scripts that fall into the 2 categories:

1. records the operations(_ops_) that occur during a stretch of time;
2. replays the recorded ops.

The two parts are not tied to each other and can be used independently for different purposes.

# How it works

## Record

How do you know which ops are performed by MongoDB? There are a lot of ways to do this. But in Flashback, we record the ops by enabling MongoDB's [profiling](http://docs.mongodb.org/manual/reference/command/profile/).

By setting the profile level to _2_ (profile all ops), we'll be able to fetch the ops information detailed enough for future replay -- except for _insert_ ops.

MongoDB intentionally avoids putting insertion details in profiling results because they don't want to have the insertion being written several times. Luckily, if a MongoDB instance is working in a "replica set", then we can complete the missing information by reading the _oplog_.

Thus, we record the ops with the following steps:

1. The script starts two threads to pull the profiling results and oplog entries for collections that we are interested in. The 2 threads work independently.
2. After fetching the entries, we'll merge the results from these two sources and have a full pictures of all the operations.

NOTE: If the oplog size is huge, fetching the first entry from oplog may take a long time (several hours) because the oplog is unindexed. After that it will catch up to present time quickly.

## Replay

With the ops being recorded, we also have a replayer to replay them in different ways:

* Replay ops with "best effort". The replayer diligently sends these ops to databases as fast as possible. This style can help us to measure the limits of databases. Please note to reduce the overhead for loading ops, we'll preload the ops to the memory and replay them as fast as possible.
* Reply ops in accordance to their original timestamps, which allows us to imitate regular traffic.

The replay module is written in Go because Python doesn't do a good job in concurrent CPU intensive tasks.

# How to use it

## Record

### Prerequisites

* The "record" module is written in python. You'll need to have pymongo, mongodb's python driver installed.
* Set MongoDB profiling level to be _2_, which captures all the ops.
* Run MongoDB in a replica set mode (even there is only one node), which allows us to access the oplog.

### Configuration

* If you are a first time user, please run `cp config.py.example config.py`.
* In `config.py`, modify it based on your need. Here are some notes:
    * We intentionally separate the servers for oplog retrieval and profiling results retrieval. As a good practice, it's better to pull oplog from secondaries. However profiling results must be pulled from the primary server.
    * `duration_secs` indicates the length for the recording.

### Start Recording

After configuration, please simply run `python record.py`.

## Replay

### Prerequisites

Install `mgo` as it is the mongodb go driver.

### Command
Basic options:

    go run main.go \
        --host=<hostname:[port]> \
        --style=[real|stress] \
        --ops_num=N \
        --workers=<workers>
        --sample_rate=<sample_rate> # sample_rate will be between (0.0, 1.0]. The default value is 1.0
        

Advanced options

    --stdout=<filename>: all regular log messages will be written to this file
    --stderr=<filename>: all error log messages will be wirtten to this file.
