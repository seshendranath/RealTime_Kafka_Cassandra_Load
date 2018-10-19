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
--conf "mapreduce.fileoutputcommitter.algorithm.version=2" \
--conf "fs.s3a.fast.upload=true" \
--conf "spark.sql.shuffle.partitions=1" \
--conf "spark.dynamicAllocation.enabled=true" \
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
--conf "spark.streaming.backpressure.enabled=true" \
--conf "spark.streaming.stopGracefullyOnShutdown=true" \
--supervise \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --class=com.indeed.dataengineering.task.Generic --runClass=com.indeed.dataengineering.task.$cname $executeMode $skipMetadata --metadata.url=jdbc:postgresql://...:5432/eravana --metadata.user=aguyyala --metadata.password=... --whitelistedTables=tbladvertiser,tblADScurrency_rates,tblADCaccounts_salesrep_commissions,tblADCadvertiser_rep_revenues,tblADCparent_company_advertisers,tblADCparent_company,tblCRMgeneric_product_credit,tblADCquota,tblACLusers,tblADCsummary_advertiser  --targetFormat=parquet  --baseLoc=s3a://indeed-data/datalake/v1/stage/binlog_events --runInterval=5 --sparkCheck
