# Real Time Streaming Data from Kafka to Cassandra using Spark Structured Streaming

This project contains all the details on how to stream Real Time events from Kafka to Cassandra.


## Usage

* Run `sbt clean assembly` to build the jar
* scp the jar to EC2 instance and EMR master node.


## Deployment

* SSH into EMR cluster and run the following command to run Spark Streaming app that consumes events from kafka and perform
real time ETL and dump the raw/aggregated data into Cassandra.

```

spark-submit \
--name RealTime_Load \
--master yarn \
--deploy-mode client \
--driver-memory=10G \
--num-executors=5 \
--executor-cores=3 \
--executor-memory=2G \
RealTime_Load-assembly-1.0-SNAPSHOT.jar -e=prod
```