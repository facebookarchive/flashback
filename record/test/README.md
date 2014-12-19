Testing record functionality
=====================================

The included scripts can stand up a local mongo replicaset on your Mac/Linux workstation.
It assumes you have mongod installed somewhere in your path.

`./test/spawnRS.sh`

To clean up later, run:

`./test/killRS.sh`

When you are ready to test record functionality, copy the included config.py into your
record directory, then run:

`./test/runQueries.sh`

`./record.py`

This will continually execute queries against the replicaset, which can be used
to verify that record is functioning.