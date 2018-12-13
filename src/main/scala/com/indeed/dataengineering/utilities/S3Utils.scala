package com.indeed.dataengineering.utilities


/**
  * Created by aguyyala on 2/16/17.
  */


import java.text.SimpleDateFormat

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, PutObjectResult}
import com.indeed.dataengineering.GenericDaemon.{conf, s3}
import org.apache.log4j.Level

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


object S3Utils extends Logging {

  log.setLevel(Level.toLevel(conf.getOrElse("logLevel", "Info")))

  def getS3Paths(sourcePath: String, timeFormat: String, startTimestamp: String, endTimestamp: String): Array[(String, Long)] = {

    val timeFormatter = new SimpleDateFormat(timeFormat)
    val ftp = new ArrayBuffer[(String, Long)]()

    val req = new ListObjectsV2Request().withBucketName(conf("s3Bucket")).withPrefix(sourcePath + "/")

    log.debug(s"Fetching s3 paths under $sourcePath between $startTimestamp and $endTimestamp")
    var result = new ListObjectsV2Result
    do {
      result = s3.listObjectsV2(req)
      ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("_spark_metadata", "success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*$e.*")) }.map(x => (conf("s3Uri") + conf("s3Bucket") + "/" + x.getKey, x.getSize))
      req.setContinuationToken(result.getNextContinuationToken)
    } while (result.isTruncated)

    ftp.toArray
  }


  def uploadToS3(s3: AmazonS3, bucket: String, key: String, contents: String): PutObjectResult = s3.putObject(bucket, key, contents)
}
