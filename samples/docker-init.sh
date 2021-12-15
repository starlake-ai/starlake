#!/usr/bin/env bash

rm -rf /samples/bin
mkdir -p /samples/bin
cp /app/starlake-spark3_2.12-0.2.8-SNAPSHOT-assembly.jar /samples/bin/
cp -r /app/spark-3.1.2-bin-hadoop3.2 /samples/bin/
HOSTNAM=`cat /etc/hostname`
echo "setup finished"
echo "to login run: docker exec -it $HOSTNAM bash"
echo "to kill session run: docker kill $HOSTNAM"
sleep 36000 # 10 hours
