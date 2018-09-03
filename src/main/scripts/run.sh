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
--files ~/log4j-spark.properties \
--conf "mapreduce.fileoutputcommitter.algorithm.version=2" \
--conf "fs.s3a.fast.upload=true" \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
--conf "dfs.block.size=1024m" \
--conf "spark.sql.shuffle.partitions=1" \
--conf "spark.cassandra.input.consistency.level=LOCAL_ONE" \
--conf "spark.cassandra.output.consistency.level=LOCAL_ONE" \
--conf "spark.dynamicAllocation.enabled=false" \
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
--supervise \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod --class=com.indeed.dataengineering.task.$cname --checkpoint




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


#nohup sh run.sh Tbladvertiser_Load 2 6 1 4 client > Tbladvertiser_Load.log 2>&1 &
#nohup sh run.sh TblADCaccounts_salesrep_commissions_Load 2 6 1 3 client > TblADCaccounts_salesrep_commissions_Load.log 2>&1 &
#nohup sh run.sh TblADCadvertiser_rep_revenues_Load 2 6 1 4 client > TblADCadvertiser_rep_revenues_Load.log 2>&1 &
#nohup sh run.sh TblCRMgeneric_product_credit_Load 2 2 1 1 client > TblCRMgeneric_product_credit_Load.log 2>&1 &
#nohup sh run.sh TblADCparent_company_advertisers_Load 2 3 1 1 client > TblADCparent_company_advertisers_Load.log 2>&1 &
#nohup sh run.sh TblADCparent_company_Load 2 3 1 1 client > TblADCparent_company_Load.log 2>&1 &
#nohup sh run.sh TblADCquota_Load 2 2 1 1 client > TblADCquota_Load.log 2>&1 &
#nohup sh run.sh TblADScurrency_rates_Load 2 2 1 1 client > TblADScurrency_rates_Load.log 2>&1 &
