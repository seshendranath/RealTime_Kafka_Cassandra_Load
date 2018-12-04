#!/usr/bin/env bash

java -cp RealTime_Load-assembly-1.0-SNAPSHOT.jar com.indeed.dataengineering.GenericDaemon -e=prod --class=com.indeed.dataengineering.task.MergeS3ToRedshift --configFile=/home/hadoop/app.conf

