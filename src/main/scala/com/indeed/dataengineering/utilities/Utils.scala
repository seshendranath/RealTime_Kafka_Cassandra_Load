package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */


import com.github.nscala_time.time.Imports.DateTime
import com.indeed.dataengineering.GenericDaemon.conf

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Utils extends  Logging {

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


  def getSetClause(opType: String): String = {
    if (opType.startsWith("i")) s"${opType}ed_records = ${opType}ed_records + 1" else s"${opType}d_records = ${opType}d_records + 1"
  }


  def getMetaQueries(className: String, db: String, tbl: String, topic: String, partition: Int, offset: BigInt): String = {
    val metaQuery =
      s"""
         |INSERT INTO metadata.streaming_metadata (job, db, tbl, topic, partition, offset)
         |VALUES ('$className', '$db', '$tbl', '$topic', $partition, $offset)
    	 """.stripMargin

    metaQuery
  }


  def getStatQueries(setClause: String, className: String, db: String, tbl: String): (String, String) = {

    val statQuery1 =
      s"""
         |UPDATE stats.streaming_stats
         |SET $setClause, total_records_processed = total_records_processed + 1
         |WHERE job = '$className' AND db = '$db' and tbl = '$tbl'
     """.stripMargin

    val statQuery2 =
      s"""
         |UPDATE stats.streaming_stats_by_hour
         |SET $setClause, total_records_processed = total_records_processed + 1
         |WHERE job = '$className' AND db = '$db' and tbl = '$tbl' AND dt = '${DateTime.now.toString("yyyy-MM-dd")}' AND hr = ${DateTime.now.getHourOfDay}
       """.stripMargin

    (statQuery1, statQuery2)
  }


  def getDatasetId(postgresql: SqlJDBC, tbl: String): Int = {
    val query = s"SELECT dataset_id FROM eravana.dataset WHERE name='$tbl'"

    log.info(s"Fetching DataSet Id for table $tbl")
    val rs = postgresql.executeQuery(query)

    var dataset_id = -1
    while (rs.next()) dataset_id = rs.getInt("dataset_id")

    dataset_id
  }


  def getColumns(postgresql: SqlJDBC, dataset_id: Int): Array[(String, String)] = {
    val query =
      s"""
         |SELECT
         |     name
         |    ,CASE WHEN data_type = 'VARCHAR' THEN data_type || '(' || max_length + 100 || ')' ELSE data_type END AS data_type
         |FROM eravana.dataset_column
         |WHERE dataset_id = $dataset_id
         |ORDER BY ordinal_position
       """.stripMargin

    log.info(s"Fetching columns for DataSet Id $dataset_id")
    val rs = postgresql.executeQuery(query)

    val result = mutable.ArrayBuffer[(String, String)]()

    while (rs.next()) result += ((rs.getString("name"), rs.getString("data_type")))

    result.toArray
  }


  def buildMetadata(postgreSql: SqlJDBC, tables: Set[String]): Map[String, Array[(String, String)]] = {
    log.info("Building Metadata")
    val result = mutable.Map[String, Array[(String, String)]]()

    tables.foreach { tbl =>
      val dataset_id = getDatasetId(postgreSql, tbl)
      val columns = getColumns(postgreSql, dataset_id)
      result += tbl -> columns
    }

    result.toMap
  }


  def timeit(s: Long, msg: String): Unit = {
    val e = System.nanoTime()
    val totalTime = (e - s) / (1e9 * 60)
    log.info(msg + " " + f"$totalTime%2.2f" + " mins")
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


  def endJobWithSuccessStatus(jobName: String, jc: JobControl, tbl: String, instanceId: String, startTimestamp: String, endTimestamp: String): Int = {
    log.info(s"End Job $jobName for object $tbl with Success Status")
    jc.endJob(instanceId, 1, startTimestamp, endTimestamp)
  }


  def endJobWithFailedStatus(jobName: String, jc: JobControl, tbl: String, instanceId: String): Int = {
    log.error(s"End Job $jobName for object $tbl with Failed Status")
    jc.endJob(instanceId, -1)
  }


  def lift[T](implicit ec: ExecutionContext, futures: Seq[Future[T]]): Seq[Future[Try[T]]] = futures.map(_.map { Success(_) }.recover { case t => Failure(t) })


  def waitAll[T](implicit ec: ExecutionContext, futures: Seq[Future[T]]): Future[Seq[Try[T]]] = Future.sequence(lift(ec, futures))

}