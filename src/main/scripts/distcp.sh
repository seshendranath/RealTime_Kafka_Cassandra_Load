#!/usr/bin/env bash

hadoop distcp -Dmapreduce.job.maxtaskfailures.per.tracker=1 -Dmapred.job.queue.name=root.dataeng -Dfs.s3a.fast.upload=true -Dfs.s3a.fast.upload.buffer=bytebuffer -Dfs.s3a.access.key=... -Dfs.s3a.secret.key=... -overwrite -delete -m $3 $1 $2

#hadoop distcp -Dmapreduce.job.maxtaskfailures.per.tracker=1 -Dmapred.job.queue.name=root.dataeng -Dfs.s3a.fast.upload=true -Dfs.s3a.fast.upload.buffer=bytebuffer -Dfs.s3a.access.key=... -Dfs.s3a.secret.key=... -m 2000 imhotep/clickanalytics/orc s3a://indeed-data/datalake/v1/prod/imhotep/clickanalytics/orc

