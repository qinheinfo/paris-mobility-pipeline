#!/bin/bash
#
# Custom entrypoint for the spark-iceberg container.
# Based on the image's original /opt/spark/entrypoint.sh, with resource
# limits added to the Thrift Server so it is not killed under memory pressure.

start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh --master "local[4]" --driver-memory 2g --driver-java-options "-Dderby.system.home=/tmp/derby"

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi