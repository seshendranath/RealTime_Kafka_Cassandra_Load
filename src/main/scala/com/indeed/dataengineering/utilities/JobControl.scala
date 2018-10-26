package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 11/09/17.
  */

import com.indeed.dataengineering.GenericDaemon.conf
import scala.collection.mutable
import com.datastax.driver.core.utils.UUIDs

class JobControl extends Logging {

  private val postgresql = new SqlJDBC("postgresql", conf("metadata.url"), conf("metadata.user"), conf("metadata.password"))

  private val db = "eravana"
  private val jobTable = "job"
  private val JobInstanceTable = "job_instance"


  /**
    * Checks whether the Job is Running or Not
    *
    * @param jobId : JobId
    * @return true if Running else false
    */
  def isRunning(jobId: Int): Boolean = {
    var isRunning = true
    val query =
      s"""
         |SELECT COUNT(*) AS is_running
         |FROM $db.$JobInstanceTable
         |WHERE job_id = $jobId AND status_flag = 0
       """.stripMargin

    val rs = postgresql.executeQuery(query)

    while (rs.next()) {
      isRunning = if (rs.getInt("is_running") > 0) true else false
    }

    isRunning
  }


  /**
    * Fetches the Active Flag of the Job
    *
    * @param jobId : Job Id
    * @return Active Flag (true or false)
    */
  def getActiveFlag(jobId: Int): Boolean = {

    var activeFlag = true

    val query =
      s"""
         |SELECT active_flag
         |FROM $db.$jobTable
         |WHERE Job_id = $jobId
       """.stripMargin

    val rs = postgresql.executeQuery(query)

    while (rs.next()) {
      activeFlag = rs.getBoolean("active_flag")
    }

    activeFlag
  }


  /**
    * Fetches last Successful job's instance details
    *
    * @param jobId : Job Id's instance to be fetched
    * @return Last Successful Instance details of a job in a Map
    */
  def getLastSuccessfulRun(jobId: Int): Map[String, String] = {

    val query =
      s"""
         |SELECT        job_id
         |             ,instance_start_time AS last_successful_instance_start_time
         |             ,etl_start_time AS last_successful_etl_start_time
         |             ,CAST(etl_start_time AS DATE) AS last_successful_etl_start_date
         |             ,etl_end_time AS  last_successful_etl_end_time
         |             ,CAST(etl_end_time AS DATE) AS  last_successful_etl_end_date
         |FROM $db.$JobInstanceTable
         |WHERE job_id = $jobId AND status_flag = 1
         |ORDER BY etl_end_time DESC LIMIT 1
       """.stripMargin

    val rs = postgresql.executeQuery(query)

    val rsmd = rs.getMetaData
    val columnsNumber = rsmd.getColumnCount

    val result = mutable.Map[String, String]()

    while (rs.next()) {
      for (i <- 1 to columnsNumber) {
        val columnValue = rs.getString(i)
        if (columnValue != null) result(rsmd.getColumnName(i)) = columnValue.toString
      }
    }

    result.toMap
  }


  /**
    * Starts Job Instance
    *
    * @param jobId : Job Id to be instantiated
    * @return Last Successful Instance Details
    */
  def startJob(jobId: Int): (String, Map[String, String]) = {

    val lsr = getLastSuccessfulRun(jobId)

    val instanceId = generateTimeUUID

    if (!getActiveFlag(jobId)) {
      log.warn("JOB IS CURRENTLY NOT ACTIVE, EXITING GRACEFULLY")
      System.exit(0)
    }

    val query =
      s"""
         |INSERT INTO $db.$JobInstanceTable(job_id, instance_id, status_flag)
         |VALUES ($jobId, '$instanceId', 0)
			 """.stripMargin

    postgresql.executeUpdate(query)

    (instanceId, lsr)

  }


  /**
    * End Job's Instance
    *
    * @param instanceId   : Instance Id to END
    * @param statusFlag   : statusFlag 1 - SUCCEEDED, 0 - Running, -1 - FAILED
    * @param ETLStartTime : ETL Start Time
    * @param ETLEndTime   : ETL End Time
    * @return 1
    */
  def endJob(instanceId: String, statusFlag: Int, ETLStartTime: String = "1900-01-01", ETLEndTime: String = "1900-01-01"): Int = {

    val query =
      s"""
         |UPDATE $db.$JobInstanceTable
         |SET  status_flag = $statusFlag
         |    ,etl_start_time = '$ETLStartTime'
         |    ,etl_end_time = '$ETLEndTime'
         |    ,instance_end_time = CURRENT_TIMESTAMP
         |WHERE instance_id = '$instanceId'
			 """.stripMargin

    postgresql.executeUpdate(query)

  }


  /**
    * Inserts Job to Job Table
    *
    * @param objectName  : Object Name
    * @param processName : Process Name
    * @param jobName     : Job Name
    */
  def insertJob(jobName: String, processName: String, objectName: String): Int = {

    log.info(s"Inserting job $jobName to the job table")
    val query =
      s"""
         |INSERT INTO  $db.$jobTable(job_name, process_name, object_name, job_type, job_group, job_frequency, description, active_flag)
         |VALUES('$jobName', '$processName', '$objectName', '${conf(s"$jobName.jobType")}', '${conf(s"$jobName.jobGroup")}', '${conf(s"$jobName.jobFrequency")}', '${conf(s"$jobName.description")}', 1)
       """.stripMargin

    log.info(s"Running Query: $query")
    postgresql.executeUpdate(query)
  }


  /**
    * Fetches Job ID from Job Table
    *
    * @param objectName  : Object Name
    * @param processName : Process Name
    * @param jobName     : Job Name
    * @return Job Id (return -1 if Job doesn't exist)
    */
  def getJobId(jobName: String, processName: String, objectName: String): Int = {

    var jobId = -1

    val query =
      s"""
         |SELECT job_id
         |FROM $db.$jobTable
         |WHERE job_name='$jobName' AND object_name = '$objectName' AND process_name = '$processName'
       """.stripMargin

    val rs = postgresql.executeQuery(query)

    while (rs.next()) {
      jobId = rs.getInt("job_id")
    }

    if (jobId == -1) {
      log.warn(s"Job $jobName NOT FOUND in the Job Table, Adding it now.")
      insertJob(jobName, processName, objectName)
      jobId = getJobId(jobName, processName, objectName)

    }

    jobId
  }


  /**
    * Generates Time Based UUID
    *
    * @return TimeUUID
    */
  def generateTimeUUID: String = UUIDs.timeBased.toString
}