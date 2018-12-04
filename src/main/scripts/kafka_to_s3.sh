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
--conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" \
--conf "spark.hadoop.fs.s3a.fast.upload=true" \
--conf "spark.sql.shuffle.partitions=1" \
--conf "spark.dynamicAllocation.enabled=true" \
--conf "spark.eventLog.enabled=false" \
--conf "spark.streaming.receiver.writeAheadLog.enable=true" \
--conf "spark.streaming.driver.writeAheadLog.closeFileAfterWrite=true" \
--conf "spark.streaming.receiver.writeAheadLog.closeFileAfterWrite=true" \
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
--conf "spark.streaming.backpressure.enabled=true" \
--conf "spark.streaming.stopGracefullyOnShutdown=true" \
--supervise \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --class=com.indeed.dataengineering.task.Generic --runClass=com.indeed.dataengineering.task.$cname $executeMode $skipMetadata --configFile=/home/hadoop/app.conf

#--offsetString="{\"maxwell\":{\"92\":1737728,\"83\":0,\"23\":0,\"95\":10683175,\"77\":0,\"86\":0,\"50\":0,\"59\":0,\"41\":0,\"32\":0,\"68\":0,\"53\":0,\"62\":0,\"35\":0,\"44\":7,\"8\":476107,\"17\":0,\"26\":2525,\"80\":0,\"89\":0,\"98\":0,\"71\":93701653,\"11\":0,\"74\":0,\"56\":0,\"38\":0,\"29\":0,\"47\":176,\"20\":0,\"2\":0,\"65\":0,\"5\":972407,\"14\":1088,\"46\":0,\"82\":0,\"91\":0,\"55\":8,\"64\":0,\"73\":33878192,\"58\":0,\"67\":0,\"85\":0,\"94\":573827,\"40\":1349,\"49\":56470565,\"4\":0,\"13\":0,\"22\":0,\"31\":0,\"76\":0,\"16\":0,\"97\":199799,\"7\":2545,\"79\":0,\"88\":0,\"70\":24915,\"43\":203116,\"52\":0,\"25\":1373235,\"34\":0,\"61\":0,\"10\":153956,\"37\":30903,\"1\":0,\"28\":0,\"19\":0,\"60\":0,\"87\":0,\"96\":0,\"69\":0,\"78\":0,\"99\":0,\"63\":0,\"90\":0,\"45\":0,\"54\":0,\"72\":0,\"81\":0,\"27\":0,\"36\":0,\"9\":0,\"18\":67261229,\"48\":0,\"21\":0,\"57\":0,\"12\":0,\"3\":0,\"84\":0,\"93\":0,\"75\":0,\"30\":0,\"39\":122461962,\"66\":0,\"15\":0,\"42\":0,\"51\":0,\"33\":0,\"24\":0,\"6\":0,\"0\":0}}"
