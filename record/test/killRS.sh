#!/bin/bash

# kill any mongos
killall mongod

sleep 5

# clean up data
rm -rf /tmp/mongo_primary
rm -rf /tmp/mongo_sec1
rm -rf /tmp/mongo_sec2

