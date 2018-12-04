#!/usr/bin/env bash


cname=$1
dm=$2
em=$3
ne=$4
ec=$5
mode=$6
executeMode=$7
skipMetadata=$8

spark-submit \
--name $cname \
--master yarn \
--deploy-mode $mode \
--driver-memory=${dm}g \
--num-executors=${ne} \
--executor-cores=${ec} \
--executor-memory=${em}g \
--files ~/log4j-spark.properties \
--class com.indeed.dataengineering.AnalyticsTaskApp \
--conf "mapreduce.fileoutputcommitter.algorithm.version=2" \
--conf "fs.s3a.fast.upload=true" \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
--conf "dfs.block.size=1024m" \
--conf "spark.sql.shuffle.partitions=1" \
--conf "spark.cassandra.input.consistency.level=LOCAL_ONE" \
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
--conf "spark.sql.ui.retainedExecutions=10" \
--conf "spark.hadoop.fs.hdfs.impl.disable.cache=true" \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.worker.cleanup.enabled=true" \
--conf "spark.ui.showConsoleProgress=false" \
--conf "spark.yarn.am.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=50  -XX:G1ReservePercent=20 -XX:+DisableExplicitGC" \
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=50  -XX:G1ReservePercent=20 -XX:+DisableExplicitGC -Dlog4j.configuration=log4j-spark.properties" \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=50  -XX:G1ReservePercent=20 -XX:+DisableExplicitGC -Dlog4j.configuration=log4j-spark.properties" \
--conf "spark.executor.heartbeatInterval=360000" \
--conf "spark.network.timeout=420000" \
--conf "spark.cleaner.ttl=120" \
--conf "spark.streaming.backpressure.enabled=false" \
--conf "spark.streaming.stopGracefullyOnShutdown=true" \
--supervise \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --class=com.indeed.dataengineering.task.Generic --runClass=com.indeed.dataengineering.task.$cname $executeMode $skipMetadata --checkpoint --checkpointBaseLoc=s3://indeed-data/datalake/v1/stage/spark/streaming/checkpoint/

#--offsetString="{\"maxwell\":{\"92\":-1,\"83\":-1,\"23\":-1,\"95\":-1,\"77\":-1,\"86\":-1,\"50\":-1,\"59\":-1,\"41\":-1,\"32\":-1,\"68\":-1,\"53\":-1,\"62\":-1,\"35\":-1,\"44\":-1,\"8\":-1,\"17\":-1,\"26\":-1,\"80\":-1,\"89\":-1,\"98\":-1,\"71\":-1,\"11\":-1,\"74\":-1,\"56\":-1,\"38\":-1,\"29\":-1,\"47\":-1,\"20\":-1,\"2\":-1,\"65\":-1,\"5\":-1,\"14\":-1,\"46\":-1,\"82\":-1,\"91\":-1,\"55\":-1,\"64\":-1,\"73\":-1,\"58\":-1,\"67\":-1,\"85\":-1,\"94\":-1,\"40\":-1,\"49\":-1,\"4\":-1,\"13\":-1,\"22\":-1,\"31\":-1,\"76\":-1,\"16\":-1,\"97\":-1,\"7\":-1,\"79\":-1,\"88\":-1,\"70\":-2,\"43\":-1,\"52\":-1,\"25\":-1,\"34\":-1,\"61\":-1,\"10\":-2,\"37\":-1,\"1\":-1,\"28\":-1,\"19\":-1,\"60\":-1,\"87\":-1,\"96\":-1,\"69\":-1,\"78\":-1,\"99\":-1,\"63\":-1,\"90\":-1,\"45\":-1,\"54\":-1,\"72\":-1,\"81\":-1,\"27\":-1,\"36\":-1,\"9\":-1,\"18\":-1,\"48\":-1,\"21\":-1,\"57\":-1,\"12\":-1,\"3\":-1,\"84\":-1,\"93\":-1,\"75\":-1,\"30\":-1,\"39\":-1,\"66\":-1,\"15\":-1,\"42\":-1,\"51\":-1,\"33\":-1,\"24\":-1,\"6\":-1,\"0\":-1}}"