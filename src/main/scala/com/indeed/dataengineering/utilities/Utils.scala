package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */


import com.github.nscala_time.time.Imports.DateTime
import com.indeed.dataengineering.GenericDaemon.conf
import com.indeed.dataengineering.models.{Column, EravanaMetadata}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Utils extends Logging {

  val redshiftKeywords = Set("partition", "offset", "type", "year", "month")

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


  def getColumns(postgresql: SqlJDBC, dataset_id: Int): Array[Column] = {
    val query =
      s"""
         |SELECT
         |     name
         |    ,CASE WHEN data_type = 'VARCHAR' THEN data_type || '(' || LEAST(max_length * 4, 65535) || ')'
         |          WHEN data_type = 'INTEGER' AND numeric_precision = 10 THEN 'BIGINT'
         |          ELSE data_type END AS data_type
         |FROM eravana.dataset_column
         |WHERE dataset_id = $dataset_id
         |ORDER BY ordinal_position
       """.stripMargin

    log.info(s"Fetching columns for DataSet Id $dataset_id")
    val rs = postgresql.executeQuery(query)

    val result = mutable.ArrayBuffer[Column]()

    while (rs.next()) result += Column(rs.getString("name"), rs.getString("data_type"))

    result.toArray
  }


  def getPrimaryKey(postgresql: SqlJDBC, dataset_id: Int): Array[String] = {
    val query =
      s"""
         |SELECT name FROM
         |(SELECT * FROM eravana.dataset_primary_key WHERE dataset_id = $dataset_id) a
         |JOIN eravana.dataset_column b ON a.dataset_id = b.dataset_id AND a.dataset_column_id = b.dataset_column_id
         |ORDER BY a.ordinal_position
       """.stripMargin

    log.info(s"Fetching primary key for DataSet Id $dataset_id")
    val rs = postgresql.executeQuery(query)

    val result = mutable.ArrayBuffer[String]()

    while (rs.next()) result += rs.getString("name")

    result.toArray
  }


  def getBatchKey(postgresql: SqlJDBC, dataset_id: Int): String = {
    val query = s"SELECT COALESCE(name, '') AS batchKey FROM eravana.dataset_column WHERE dataset_id = $dataset_id AND is_batch_key IS TRUE ORDER BY ordinal_position LIMIT 1"

    log.info(s"Fetching batch key for dataset_id $dataset_id")
    val rs = postgresql.executeQuery(query)

    var batchKey = ""
    while (rs.next()) batchKey = rs.getString("batchKey")

    batchKey
  }


  def buildMetadata(postgresql: SqlJDBC, tables: Set[String]): Map[String, EravanaMetadata] = {
    log.info("Building Metadata")
    val result = mutable.Map[String, EravanaMetadata]()

    tables.foreach { tbl =>
      val dataset_id = getDatasetId(postgresql, tbl)
      val columns = getColumns(postgresql, dataset_id)
      val primaryKey = getPrimaryKey(postgresql, dataset_id)
      val batchKey = getBatchKey(postgresql, dataset_id)
      result += tbl -> EravanaMetadata(columns, primaryKey, batchKey)
    }

    result.toMap
  }


  def escapeColName(c: String): String = if (redshiftKeywords contains c) s""""$c"""" else c


  def checkIfDataPresent(redshift: SqlJDBC, schema: String, tbl: String): Boolean = {
    val query = s"SELECT COUNT(*) AS cnt FROM $schema.$tbl"
    val rs = redshift.executeQuery(query)

    var dataPresent = true
    while (rs.next()) {
      val cnt = rs.getInt("cnt")
      log.info(s"Record Count of $schema.$tbl: $cnt")
      dataPresent = if (cnt > 0) true else false
    }

    dataPresent
  }


  def generateRowHashColStr(metadata: Map[String, EravanaMetadata], tbl: String): String = {
    val colStr = metadata(tbl).columns.map(c => if (c.dataType == "BOOLEAN") s"COALESCE(CAST(CAST(${escapeColName(c.name)} AS INTEGER) AS VARCHAR), '')" else s"COALESCE(CAST(${escapeColName(c.name)} AS VARCHAR), '')").mkString(" + ")

    s"MD5($colStr)"
  }


  def generateRankColStr(metadata: Map[String, EravanaMetadata], tbl: String): String = {
    val pk = metadata(tbl).primaryKey.map(c => escapeColName(c)).mkString(",")

    s"ROW_NUMBER() OVER (PARTITION BY $pk ORDER BY binlog_file DESC, binlog_position DESC, binlog_timestamp DESC)"
  }


  def generateColStr(metadata: Map[String, EravanaMetadata], tbl: String): String = {
    metadata(tbl).columns.map(c => escapeColName(c.name)).mkString(",")
  }


  def generateDeDupSelectQuery(metadata: Map[String, EravanaMetadata], stageSchema: String, tbl: String): String = {
    val colStr = generateColStr(metadata, tbl)
    val rankColStr = generateRankColStr(metadata, tbl)

    s"""
       |SELECT
       |      $colStr, op_type, binlog_timestamp
       |FROM (SELECT
       |           $colStr, op_type, binlog_timestamp, $rankColStr AS rank
       |      FROM $stageSchema.$tbl
       |      ) a
       |WHERE rank  = 1
     """.stripMargin
  }


  def generateCreateTempTblQuery(metadata: Map[String, EravanaMetadata], stageSchema: String, tbl: String): String = {
    val deDupSelectQuery = generateDeDupSelectQuery(metadata, stageSchema, tbl)

    s"CREATE TEMP TABLE $tbl AS $deDupSelectQuery"
  }


  def generateDeleteQuery(metadata: Map[String, EravanaMetadata], finalSchema: String, tbl: String, historyKeyword: String = ""): String = {

    val (finalTbl, historyDateStr) = if (historyKeyword.nonEmpty) (tbl + historyKeyword, s" AND DATE(stg.binlog_timestamp) = $finalSchema.${tbl + historyKeyword}.event_date") else (tbl, "")

    val pkStr = metadata(tbl).primaryKey.map(c => escapeColName(c)).map(c => s"""stg.$c = $finalSchema.$finalTbl.$c""").mkString(" AND ")

    val batchKey = metadata(tbl).batchKey
    val bkStr = if (batchKey.nonEmpty) s"AND COALESCE($finalSchema.$finalTbl.$batchKey, '0001-01-01 00:00:00') <= COALESCE(stg.$batchKey, '0001-01-01 00:00:00')" else ""

    s"DELETE FROM $finalSchema.$finalTbl USING $tbl stg WHERE $pkStr $historyDateStr $bkStr"
  }


  def generateInsertQuery(metadata: Map[String, EravanaMetadata], finalSchema: String, tbl: String, historyKeyword: String = ""): String = {
    val colStr = generateColStr(metadata, tbl)
    val rowHashStr = generateRowHashColStr(metadata, tbl)

    val (finalTbl, eventDt, eventDtCol) = if (historyKeyword.nonEmpty) (tbl + historyKeyword, "event_date, ", "DATE(binlog_timestamp) AS event_date,") else (tbl, "", "")

    s"""
       |INSERT INTO $finalSchema.$finalTbl ($eventDt $colStr, row_hash, is_deleted, etl_deleted_timestamp, etl_inserted_timestamp, etl_updated_timestamp)
       |SELECT
       |      $eventDtCol
       |      $colStr
       |     ,$rowHashStr AS row_hash
       |     ,CASE WHEN op_type = 'delete' THEN true ELSE false END AS is_deleted
       |     ,CASE WHEN op_type = 'delete' THEN binlog_timestamp ELSE NULL END AS etl_deleted_timestamp
       |     ,CURRENT_TIMESTAMP AS etl_inserted_timestamp
       |     ,CURRENT_TIMESTAMP AS etl_updated_timestamp
       |FROM $tbl
     """.stripMargin
  }


  def generateTruncateQuery(stageSchema: String, tbl: String): String = {
    s"TRUNCATE TABLE $stageSchema.$tbl"
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


  def endJob(jc: JobControl, jobName: String, processName: String, statusFlag: Int, tbl: String, instanceId: String, startTimestamp: String = "1900-01-01", endTimestamp: String = "1900-01-01"): Int = {
    log.info(s"End Job $jobName, $processName process for object $tbl with ${if (statusFlag == 1) "Success" else "Failed"} Status")
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