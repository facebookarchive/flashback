#!/bin/bash

# setup directories
mkdir -p /tmp/mongo_primary/data
mkdir -p /tmp/mongo_sec1/data
mkdir -p /tmp/mongo_sec2/data
mkdir -p /tmp/mongo_primary/log
mkdir -p /tmp/mongo_sec1/log
mkdir -p /tmp/mongo_sec2/log

# launch mongo
mongod --fork --port 30000 --dbpath /tmp/mongo_primary/data --logpath /tmp/mongo_primary/log/mongodb.log --smallfiles --journal --oplogSize 100 --replSet testenv
mongod --fork --port 30001 --dbpath /tmp/mongo_sec1/data --logpath /tmp/mongo_sec1/log/mongodb.log --smallfiles --journal --oplogSize 100 --replSet testenv
mongod --fork --port 30002 --dbpath /tmp/mongo_sec2/data --logpath /tmp/mongo_sec2/log/mongodb.log --smallfiles --journal --oplogSize 100 --replSet testenv

sleep 5

# initiate a replicaset
echo 'rs.initiate({"_id": "testenv", "members": [ { "_id": 0, "host": "localhost:30000", "priority": 3 }, {"_id":1, "host": "localhost:30001", "priority": 2}, {"_id":2, "host":"localhost:30002", "priority": 2 }]})' | mongo localhost:30000

# sleep long enough to let RS initiate complete
sleep 30

# enable profiling on all servers
echo 'db.setProfilingLevel(2)' | mongo localhost:30000/foo
echo 'rs.slaveOk(); db.setProfilingLevel(2)' | mongo localhost:30001/foo
echo 'rs.slaveOk(); db.setProfilingLevel(2)' | mongo localhost:30002/foo
echo 'db.setProfilingLevel(2)' | mongo localhost:30000/bar
echo 'rs.slaveOk(); db.setProfilingLevel(2)' | mongo localhost:30001/bar
echo 'rs.slaveOk(); db.setProfilingLevel(2)' | mongo localhost:30002/bar
