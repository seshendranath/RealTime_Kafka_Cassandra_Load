#!/usr/bin/env bash

spark-submit \
--name RealTime_Load \
--master yarn \
--deploy-mode client \
--driver-memory=10G \
--num-executors=2 \
--executor-cores=1 \
--executor-memory=5G \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod

cname=$1
dm=$2
em=$3
ne=$4
ec=$5

spark-submit \
--name $cname \
--master yarn \
--deploy-mode client \
--driver-memory=${dm}G \
--num-executors=${ne} \
--executor-cores=${ec} \
--executor-memory=${em}G \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --class=com.indeed.dataengineering.task.$cname


db=$1
tbl=$2
pk=$3

spark-submit \
--name RealTime_Load \
--master yarn \
--deploy-mode client \
--driver-memory=10G \
--num-executors=2 \
--executor-cores=1 \
--executor-memory=5G \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --db=$db --table=$tbl --pk=$pk
