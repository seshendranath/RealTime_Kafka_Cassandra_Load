package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import java.util.concurrent.Executors

import com.github.nscala_time.time.Imports.DateTime
import com.indeed.dataengineering.GenericDaemon.{conf, s3}
import com.indeed.dataengineering.models.EravanaMetadata
import com.indeed.dataengineering.utilities.{JobControl, Logging, SqlJDBC}
import com.indeed.dataengineering.utilities.S3Utils._
import com.indeed.dataengineering.utilities.Utils._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}


class MergeS3ToRedshift extends Logging {

  val jobName: String = this.getClass.getSimpleName

  val s3Bucket = conf("s3Bucket")

  log.info("Creating Postgresql connection")
  val postgresql = new SqlJDBC("postgresql", conf("metadata.url"), conf("metadata.user"), conf("metadata.password"))

  log.info("Creating Redshift connection")
  val redshift = new SqlJDBC("redshift", conf("redshift.url"), conf("redshift.user"), conf("redshift.password"))

  val jc = new JobControl

  val timeFormat = "yyyy-MM-dd HH:mm:ss"

  val runInterval: Int = conf.getOrElse("runInterval", "5").toInt

  val metadata: Map[String, EravanaMetadata] = buildMetadata(postgresql, conf("whitelistedTables").split(",").toSet)

  def run(): Unit = {

    log.info(s"Job Name: $jobName")

    val whitelistedTables = conf("whitelistedTables").split(",").toSet.toArray

    val executorService = Executors.newFixedThreadPool(whitelistedTables.length)
    val executionContext = ExecutionContext.fromExecutorService(executorService)

    // TODO: Handle SIGTERM/SIGINT to exit gracefully
    try {
      while (true) {

        if (conf.getOrElse("runSequentially", "false").toBoolean) {
          whitelistedTables.foreach(tbl => process(tbl))
        } else {
          val res = go(executionContext, whitelistedTables)

          Await.result(waitAll(executionContext, res), scala.concurrent.duration.Duration.Inf).foreach {
            case Success(_) =>
            case Failure(e) => throw e
          }
        }

        log.info(s"Sleeping for $runInterval minutes...")
        Thread.sleep(runInterval * 60 * 1000)
      }
    } catch {
      case e: Exception => throw e
    } finally {
      log.info("Shutting Down Executor Service and Execution Context")
      executorService.shutdown()
      executionContext.shutdown()
    }

  }


  def process(tbl: String): Unit = {
    val jobId = jc.getJobId(jobName, conf(s"$jobName.processName"), tbl)
    log.info(s"Job Id for job $jobName and object $tbl: $jobId")

    log.info(s"Start Job for $jobName and object $tbl")
    val (instanceId, lastSuccessfulRunDetails) = jc.startJob(jobId)
    log.info(s"Last Successful Run Details for job $jobName and object $tbl: $lastSuccessfulRunDetails")

    val defaultStartTimestamp = DateTime.now.minusDays(1).toString(timeFormat)
    val startTimestamp = lastSuccessfulRunDetails.getOrElse("last_successful_etl_end_time", defaultStartTimestamp)
    val endTimestamp = DateTime.now.toString(timeFormat)

    log.info(s"Start and End Timestamps for $jobName and $tbl: $startTimestamp and $endTimestamp")
    try {

      val sourcePath = conf("baseLoc") + s"/$tbl"

      val ftp = getS3Paths(sourcePath, timeFormat, startTimestamp, endTimestamp)

      if (ftp.isEmpty) {
        log.info(s"No files to copy...")
        endJobWithSuccessStatus(jobName, jc, tbl, instanceId, startTimestamp, endTimestamp)
        return
      }

      val manifestFileContents = getManifestFileContents(ftp)

      val manifestFileName = getManifestFileName(tbl, endTimestamp, instanceId)

      log.info(s"Uploading manifest file $manifestFileName to s3")
      uploadToS3(s3, s3Bucket, manifestFileName, manifestFileContents)

      // TODO: Separate out copy and merge process so if one fails it doesn't affect the timestamps of another
      runCopyCmd(redshift, tbl, manifestFileName)

      merge(redshift, tbl)

    } catch {
      case e: Exception =>
        endJobWithFailedStatus(jobName, jc, tbl, instanceId)
        throw e
    }

    endJobWithSuccessStatus(jobName, jc, tbl, instanceId, startTimestamp, endTimestamp)
  }


  def getManifestFileContents(ftp: Array[(String, Long)]): String = """{ "entries" : [ """ + ftp.map { f => s"""{"url":"${f._1}", "mandatory":true, "meta":{ "content_length": ${f._2} }}""" }.mkString(",\n") + """ ] }"""


  def getManifestFileName(tbl: String, endTimestamp: String, instanceId: String): String = conf("manifestLoc") + s"/$tbl/${tbl}_${endTimestamp.replaceAll("[-, ,:]", "_")}_$instanceId.manifest"


  def go(implicit ec: ExecutionContext, whitelistedTables: Array[String]): Seq[Future[Unit]] = {
    val F: Seq[Future[Unit]] = for (tbl <- whitelistedTables) yield {
      Future {
        process(tbl)
      }
    }
    F
  }


  def merge(redshift: SqlJDBC, tbl: String): Int = {
    val stageSchema = conf("redshift.schema")
    val finalSchema = conf("redshift.final.schema")

    val createTempTblQuery = generateCreateTempTblQuery(metadata, stageSchema, tbl)
    //    redshift.executeUpdate(createTempTblQuery)

    val deleteQuery = generateDeleteQuery(metadata, finalSchema, tbl)
    //    redshift.executeUpdate(deleteQuery)

    val insertQuery = generateInsertQuery(metadata, finalSchema, tbl)
    //    redshift.executeUpdate(insertQuery)

    val truncateQuery = generateTruncateQuery(stageSchema, tbl)
    //    redshift.executeUpdate(insertQuery)

    val transaction =
      s"""
         |BEGIN TRANSACTION;
         |$createTempTblQuery;
         |$deleteQuery;
         |$insertQuery;
         |$truncateQuery;
         |END TRANSACTION;
       """.stripMargin

    val s = System.nanoTime()
    val res = redshift.executeUpdate(transaction)
    timeit(s, s"Time took to complete Merge for $tbl")
    res
  }

}
