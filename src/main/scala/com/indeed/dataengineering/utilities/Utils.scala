package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */


import com.indeed.dataengineering.GenericDaemon.conf
import com.indeed.dataengineering.models.Column
import org.apache.log4j.Level

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Utils extends Logging {

  log.setLevel(Level.toLevel(conf.getOrElse("logLevel", "Info")))

  val redshiftKeywords = Set("partition", "offset", "type", "year", "month", "percent")

  val quoteTypes = Set("StringType", "DateType", "TimestampType")

  def convertBoolToInt(dtype: String): String = if (dtype == "BooleanType") "IntegerType" else dtype

  def sparkToScalaDataType(dataType: String): String = {
    val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

    dataType match {
      case "StringType" => "String"
      case "LongType" => "BigInt"
      case "DoubleType" => "Double"
      case "IntegerType" => "Int"
      case "DateType" => "Date"
      case "TimestampType" => "Timestamp"
      case "BooleanType" => "Int"
      case decimalPattern() => "BigDecimal" //"Double"// dataType.replace("Type", "").toLowerCase
      case _ => "String"
    }
  }


  def getScalaType(pk: Set[String], col: String, dtype: String): String = {
    if ((quoteTypes contains dtype) || (pk contains col)) sparkToScalaDataType(dtype) else "Option[" + sparkToScalaDataType(dtype) + "]"
  }


  def sparkToCassandraDataType(dataType: String): String = {
    val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

    dataType match {
      case "StringType" => "text"
      case "LongType" => "bigint"
      case "DoubleType" => "double"
      case "IntegerType" => "int"
      case "DateType" => "date"
      case "TimestampType" => "timestamp"
      case "BooleanType" => "boolean"
      case decimalPattern() => "decimal" // "double"// dataType.replace("Type", "").toLowerCase
      case _ => "text"
    }
  }


  def timeit(s: Long, msg: String): Unit = {
    val e = System.nanoTime()
    val totalTime = (e - s) / (1e9 * 60)
    log.debug(msg + " " + f"$totalTime%2.2f" + " mins")
  }


  def postgresqlToRedshiftDataType(dataType: String): String = {

    val varcharPattern = """VARCHAR\(\d+\)""".r

    dataType match {
      case "DATETIME" => "TIMESTAMP WITHOUT TIME ZONE"
      case "TIME" => "VARCHAR(50)"
      case "DATE" => "DATE"
      case "SMALLINT" => "INTEGER"
      case "TIMESTAMP" => "TIMESTAMP WITHOUT TIME ZONE"
      case "FLOAT" => "DOUBLE PRECISION"
      case "INTEGER" => "INTEGER"
      case varcharPattern() => dataType
      case "NUMERIC" => "DOUBLE PRECISION"
      case "BIGINT" => "BIGINT"
      case "UUID" => "VARCHAR(50)"
      case "BOOLEAN" => "BOOLEAN"
      case "DOUBLE" => "DOUBLE PRECISION"
      case "YEAR" => "INTEGER"
      case _ => "VARCHAR(1000)"
    }
  }


  def runCopyCmd(redshift: SqlJDBC, tbl: String, manifestFileName: String): Int = {
    val copyCmd = s"COPY ${conf("redshift.schema")}.${tbl.toLowerCase} FROM 's3://${conf("s3Bucket")}/$manifestFileName' IAM_ROLE '${conf("iamRole")}' MANIFEST FORMAT AS PARQUET"
    val s = System.nanoTime()
    val res = redshift.executeUpdate(copyCmd)
    timeit(s, s"Time took to complete Copy for $tbl")
    res
  }


  def endJob(jc: JobControl, jobName: String, processName: String, statusFlag: Int, tbl: String, instanceId: String, startTimestamp: String = "1900-01-01", endTimestamp: String = "1900-01-01"): Int = {
    log.debug(s"End Job $jobName, $processName process for object $tbl with ${if (statusFlag == 1) "Success" else "Failed"} Status")
    jc.endJob(instanceId, statusFlag, startTimestamp, endTimestamp)
  }


  def lift[T](implicit ec: ExecutionContext, futures: Seq[Future[T]]): Seq[Future[Try[T]]] = futures.map(_.map {
    Success(_)
  }.recover { case t => Failure(t) })


  def waitAll[T](implicit ec: ExecutionContext, futures: Seq[Future[T]]): Future[Seq[Try[T]]] = Future.sequence(lift(ec, futures))


  def transformations(c: Column): String = {
    if (c.dataType == "BOOLEAN") {
      s"CAST(${c.name} AS Boolean) AS ${c.name}"
    } else if (c.dataType == "TIMESTAMP") {
      s"FROM_UTC_TIMESTAMP(${c.name}, 'CST') AS ${c.name}"
    } else {
      c.name
    }
  }
}