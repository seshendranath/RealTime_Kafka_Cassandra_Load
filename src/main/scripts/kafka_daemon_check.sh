#!/usr/bin/env bash

echo "Kafka Started"
while [ True ]
do
    if [ `ps -ef | grep -v grep | grep kafka | wc -l` -ne 2 ]
    then
        echo "Kafka FAILED"
        echo "Kafka Daemon Failed" | mailx -s "Kafka Daemon Failed" -r aguyyala@indeed.com aguyyala@indeed.com
        break
    fi
    sleep 5
done
