#!/usr/bin/env bash

node=$1

sbt assembly
scp ../RealTime_Kafka_Cassandra_Load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar hadoop@$node:
