#!/bin/bash

# insert a doc and query it on all replicas
while true; do
        date=$(date)
        echo "db.foo.insert({a: \"${date}\"})" | mongo localhost:30000/foo > /dev/null 2>&1
        echo "db.bar.insert({b: \"${date}\"})" | mongo localhost:30000/bar > /dev/null 2>&1
        echo "db.foo.find({a: \"${date}\"})"  | mongo localhost:30000/foo > /dev/null 2>&1
	echo "db.bar.find({b: \"${date}\"}).sort({a: 1})"  | mongo localhost:30000/bar > /dev/null 2>&1
        echo "rs.slaveOk(); db.foo.find({a: \"${date}\"})" | mongo localhost:30001/foo > /dev/null 2>&1
        echo "rs.slaveOk(); db.bar.find({b: \"${date}\"})" | mongo localhost:30001/bar > /dev/null 2>&1
        echo "rs.slaveOk(); db.foo.find({a: \"${date}\"})" | mongo localhost:30002/foo > /dev/null 2>&1
        echo "rs.slaveOk(); db.bar.find({b: \"${date}\"})" | mongo localhost:30002/bar > /dev/null 2>&1
        sleep .1
done; 
