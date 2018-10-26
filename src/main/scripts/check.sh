#!/usr/bin/env bash

job=$1

echo "Started: "$job
while [ True ]
do
    if [ `ps -ef | grep -v grep | grep "$job" | wc -l` -ne 1 ]
    then
        echo "FAILED: "$job
        cat $job.log | mailx -a $job.log -s "Spark Streaming Job "$job" Failed" -r aguyyala@indeed.com aguyyala@indeed.com
        break
    fi
    sleep 5
done