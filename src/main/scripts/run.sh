#!/usr/bin/env bash

spark-submit \
--name RealTime_Load \
--master yarn \
--deploy-mode client \
--driver-memory=10g \
--num-executors=2 \
--executor-cores=1 \
--executor-memory=5g \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod

cname=$1
dm=$2
em=$3
ne=$4
ec=$5
mode=$6

spark-submit \
--name $cname \
--master yarn \
--deploy-mode $mode \
--driver-memory=${dm}g \
--num-executors=${ne} \
--executor-cores=${ec} \
--executor-memory=${em}g \
--conf "hive.exec.dynamic.partition=true" \
--conf "hive.exec.dynamic.partition.mode=nonstrict" \
--conf "mapreduce.fileoutputcommitter.algorithm.version=2" \
--conf "fs.s3a.fast.upload=true" \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "dfs.block.size=1024m" \
--conf "spark.sql.shuffle.partitions=2" \
--conf "spark.cassandra.output.consistency.level=LOCAL_ONE" \
--conf "spark.dynamicAllocation.enabled=false" \
--conf "spark.eventLog.enabled=false" \
--conf "spark.streaming.receiver.writeAheadLog.enable=false" \
--conf "spark.streaming.unpersist=true" \
--conf "spark.streaming.ui.retainedBatches=10" \
--conf "spark.ui.retainedJobs=10" \
--conf "spark.ui.retainedStages=10" \
--conf "spark.worker.ui.retainedExecutors=10" \
--conf "spark.worker.ui.retainedDrivers=10" \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.yarn.driver.memoryOverhead=1024" \
--conf "spark.yarn.executor.memoryOverhead=1024" \
--conf "spark.worker.cleanup.enabled=true" \
--conf "spark.ui.showConsoleProgress=false" \
--conf "spark.yarn.am.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=50  -XX:G1ReservePercent=20 -XX:+DisableExplicitGC" \
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=50  -XX:G1ReservePercent=20 -XX:+DisableExplicitGC" \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=50  -XX:G1ReservePercent=20 -XX:+DisableExplicitGC" \
--conf "spark.executor.heartbeatInterval=360000" \
--conf "spark.network.timeout=420000" \
--supervise \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --class=com.indeed.dataengineering.task.$cname



db=$1
tbl=$2
pk=$3

spark-submit \
--name RealTime_Load \
--master yarn \
--deploy-mode client \
--driver-memory=10g \
--num-executors=2 \
--executor-cores=1 \
--executor-memory=5g \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --db=$db --table=$tbl --pk=$pk
