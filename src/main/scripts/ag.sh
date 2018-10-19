#!/usr/bin/env bash

db=$1
tbl=$2
pk=$3
sp=$4

spark-submit \
--name RealTime_Load \
--master yarn \
--deploy-mode client \
--driver-memory=1g \
--num-executors=1 \
--executor-cores=1 \
--executor-memory=1g \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --db=$db --table=$tbl --pk=$pk --sourcePath=$sp
