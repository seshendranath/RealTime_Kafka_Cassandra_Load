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