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
    try {
      copy(tbl)
      merge(tbl)
    } catch {
      case e: Exception => throw e
    }
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


  def copy(tbl: String): Unit = {
    val processName = "copy"

    val jobId = jc.getJobId(jobName, processName, tbl)
    log.info(s"Job Id for job $jobName, $processName process and object $tbl: $jobId")

    log.info(s"Start $processName process for $jobName and object $tbl")
    val (instanceId, lastSuccessfulRunDetails) = jc.startJob(jobId)
    log.info(s"Last Successful Run Details for $processName process of $jobName and object $tbl: $lastSuccessfulRunDetails")

    val defaultStartTimestamp = DateTime.now.minusDays(1).toString(timeFormat)
    val startTimestamp = lastSuccessfulRunDetails.getOrElse("last_successful_etl_end_time", defaultStartTimestamp)
    val endTimestamp = DateTime.now.toString(timeFormat)

    try {
      log.info(s"Start and End Timestamps for $processName process of $jobName and $tbl: $startTimestamp and $endTimestamp")

      val sourcePath = conf("baseLoc") + s"/$tbl"

      val ftp = getS3Paths(sourcePath, timeFormat, startTimestamp, endTimestamp)

      if (ftp.isEmpty) {
        log.info(s"No files to copy...")
        endJob(jc, jobName, processName, 1, tbl, instanceId, startTimestamp, endTimestamp)
        return
      }

      val manifestFileContents = getManifestFileContents(ftp)

      val manifestFileName = getManifestFileName(tbl, endTimestamp, instanceId)

      log.info(s"Uploading manifest file $manifestFileName to s3")
      uploadToS3(s3, s3Bucket, manifestFileName, manifestFileContents)

      runCopyCmd(redshift, tbl, manifestFileName)
    } catch {
      case e: Exception =>
        endJob(jc, jobName, processName, -1, tbl, instanceId)
        throw e
    }

    endJob(jc, jobName, processName, 1, tbl, instanceId, startTimestamp, endTimestamp)
  }


  def merge(tbl: String): Unit = {
    val processName = "merge"

    val jobId = jc.getJobId(jobName, processName, tbl)
    log.info(s"Job Id for job $jobName, $processName process and object $tbl: $jobId")

    log.info(s"Start $processName process for $jobName and object $tbl")
    val (instanceId, _) = jc.startJob(jobId)

    try {
      val stageSchema = conf("redshift.schema")
      val finalSchema = conf("redshift.final.schema")

      val createTempTblQuery = generateCreateTempTblQuery(metadata, stageSchema, tbl)

      val deleteQuery = generateDeleteQuery(metadata, finalSchema, tbl)

      val insertQuery = generateInsertQuery(metadata, finalSchema, tbl)

      val truncateQuery = generateTruncateQuery(stageSchema, tbl)

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
      redshift.executeUpdate(transaction)
      timeit(s, s"Time took to complete Merge for $tbl")
    } catch {
      case e: Exception =>
        endJob(jc, jobName, processName, -1, tbl, instanceId)
        throw e
    }

    endJob(jc, jobName, processName, 1, tbl, instanceId)
  }

}
