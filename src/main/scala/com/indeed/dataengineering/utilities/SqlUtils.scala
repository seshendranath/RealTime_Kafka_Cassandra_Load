package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */


import com.github.nscala_time.time.Imports.DateTime
import com.indeed.dataengineering.LoadConfig.conf
import com.indeed.dataengineering.models.{Column, EravanaMetadata}
import com.indeed.dataengineering.utilities.Utils.redshiftKeywords
import org.apache.log4j.Level

import scala.collection.mutable


object SqlUtils extends Logging {

  log.setLevel(Level.toLevel(conf.getOrElse("logLevel", "Info")))

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


  def getPrimaryKey(postgresql: SqlJDBC, dataset_id: Int): Array[Column] = {
    val query =
      s"""
         |SELECT
         |      name
         |     ,CASE WHEN data_type = 'VARCHAR' THEN data_type || '(' || LEAST(max_length * 4, 65535) || ')'
         |          WHEN data_type = 'INTEGER' AND numeric_precision = 10 THEN 'BIGINT'
         |          ELSE data_type END AS data_type
         |FROM
         |(SELECT * FROM eravana.dataset_primary_key WHERE dataset_id = $dataset_id) a
         |JOIN eravana.dataset_column b ON a.dataset_id = b.dataset_id AND a.dataset_column_id = b.dataset_column_id
         |ORDER BY a.ordinal_position
       """.stripMargin

    log.info(s"Fetching primary key for DataSet Id $dataset_id")
    val rs = postgresql.executeQuery(query)

    val result = mutable.ArrayBuffer[Column]()

    while (rs.next()) result += Column(rs.getString("name"), rs.getString("data_type"))

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
      log.debug(s"Record Count of $schema.$tbl: $cnt")
      dataPresent = if (cnt > 0) true else false
    }

    dataPresent
  }


  def generateRowHashColStr(metadata: Map[String, EravanaMetadata], tbl: String): String = {
    val colStr = metadata(tbl).columns.map(c => if (c.dataType == "BOOLEAN") s"COALESCE(CAST(CAST(${escapeColName(c.name)} AS INTEGER) AS VARCHAR), '')" else s"COALESCE(CAST(${escapeColName(c.name)} AS VARCHAR), '')").mkString(" + ")

    s"MD5($colStr)"
  }


  def generateRankColStr(metadata: Map[String, EravanaMetadata], tbl: String): String = {
    val pk = metadata(tbl).primaryKey.map(c => escapeColName(c.name)).mkString(",")

    s"ROW_NUMBER() OVER (PARTITION BY $pk ORDER BY binlog_file DESC, binlog_position DESC, binlog_timestamp DESC)"
  }


  def generateBatchRankColStr(metadata: Map[String, EravanaMetadata], tbl: String, historyKeyword: String = ""): String = {
    val pk = metadata(tbl).primaryKey.map(c => escapeColName(c.name)).mkString(",")
    val batchKey = metadata(tbl).batchKey

    if (historyKeyword.nonEmpty) {
      s"ROW_NUMBER() OVER (PARTITION BY $pk ORDER BY COALESCE(final_$batchKey, '0001-01-01 00:00:00') DESC, COALESCE(hist_final_$batchKey, '0001-01-01 00:00:00') DESC)"
    } else  s"ROW_NUMBER() OVER (PARTITION BY $pk ORDER BY COALESCE(final_$batchKey, '0001-01-01 00:00:00') DESC)"
  }


  def generateColStr(metadata: Map[String, EravanaMetadata], tbl: String): String = {
    metadata(tbl).columns.map(c => escapeColName(c.name)).mkString(",")
  }


  def generateDeDupSelectQuery(metadata: Map[String, EravanaMetadata], stageSchema: String, finalSchema: String, tbl: String, historyKeyword: String = ""): String = {
    val stgAlias = "stg"
    val finalAlias = "final"
    val histFinalAlias = "hist_final"

    val colStr = generateColStr(metadata, tbl)
    val stgColStr = colStr.split(",").map(c => s"$stgAlias.$c").mkString(",")
    val rankColStr = generateRankColStr(metadata, tbl)
    val batchRankColStr= generateBatchRankColStr(metadata, tbl, historyKeyword)

    val pkStr = metadata(tbl).primaryKey.map(c => escapeColName(c.name)).map(c => s"""$stgAlias.$c = $finalAlias.$c""").mkString(" AND ")
    val batchKey = metadata(tbl).batchKey
    val bkStr = if (batchKey.nonEmpty) s"COALESCE($finalAlias.$batchKey, '0001-01-01 00:00:00') <= COALESCE($stgAlias.$batchKey, '0001-01-01 00:00:00')" else ""

    val (histLeftJoin, histBkStr, histFinalBatchKey, histFinalDeleteFlag) = if (historyKeyword.nonEmpty) {
      val hLeftJoin = s"LEFT JOIN $finalSchema.${tbl + historyKeyword} $histFinalAlias ON ${pkStr.replace(s"$finalAlias.", s"$histFinalAlias.")} AND DATE($stgAlias.$batchKey) = $histFinalAlias.event_date"
      val hBkStr = s",CASE WHEN ${bkStr.replace(s"$finalAlias.", s"$histFinalAlias.")} THEN TRUE ELSE FALSE END AS hist_final_delete_flag"
      (hLeftJoin, hBkStr, s",hist_final.$batchKey AS hist_final_$batchKey", "hist_final_delete_flag,")
    } else ("", "", "", "")


    s"""
       |SELECT
       |     $colStr, op_type, binlog_timestamp, etl_inserted_timestamp, $histFinalDeleteFlag final_delete_flag
       |FROM
       |(
       |SELECT
       |      $colStr, op_type, binlog_timestamp, etl_inserted_timestamp, $histFinalDeleteFlag final_delete_flag, $batchRankColStr AS final_rank
       |FROM
       |(SELECT
       |       $stgColStr, op_type, binlog_timestamp, final.etl_inserted_timestamp AS etl_inserted_timestamp
       |      ,final.$batchKey AS final_$batchKey
       |      $histFinalBatchKey
       |      ,CASE WHEN $bkStr THEN TRUE ELSE FALSE END AS final_delete_flag
       |      $histBkStr
       |FROM
       |(SELECT
       |       $colStr, op_type, binlog_timestamp
       |FROM (SELECT
       |           $colStr, op_type, binlog_timestamp, $rankColStr AS rank
       |      FROM $stageSchema.$tbl
       |      ) tmp1
       |WHERE rank  = 1) stg LEFT JOIN $finalSchema.$tbl final ON $pkStr
       |$histLeftJoin
       |)) WHERE final_rank = 1
     """.stripMargin
  }


  def generateCreateTempTblQuery(metadata: Map[String, EravanaMetadata], stageSchema: String, finalSchema: String, tbl: String, historyKeyword: String = ""): String = {
    val deDupSelectQuery = generateDeDupSelectQuery(metadata, stageSchema, finalSchema, tbl, historyKeyword)

    s"CREATE TEMP TABLE $tbl AS $deDupSelectQuery"
  }


  def generateDeleteQuery(metadata: Map[String, EravanaMetadata], finalSchema: String, tbl: String, historyKeyword: String = ""): String = {
    val batchKey = metadata(tbl).batchKey

    val (finalTbl, historyDateStr, deleteFlagStr) = if (historyKeyword.nonEmpty) (tbl + historyKeyword, s" AND DATE(stg.$batchKey) = $finalSchema.${tbl + historyKeyword}.event_date", "AND hist_final_delete_flag") else (tbl, "", "AND final_delete_flag")

    val pkStr = metadata(tbl).primaryKey.map(c => escapeColName(c.name)).map(c => s"""stg.$c = $finalSchema.$finalTbl.$c""").mkString(" AND ")

    //    val batchKey = metadata(tbl).batchKey
    //    val bkStr = if (batchKey.nonEmpty) s"AND COALESCE($finalSchema.$finalTbl.$batchKey, '0001-01-01 00:00:00') <= COALESCE(stg.$batchKey, '0001-01-01 00:00:00')" else ""

    s"DELETE FROM $finalSchema.$finalTbl USING $tbl stg WHERE $pkStr $historyDateStr $deleteFlagStr"
  }


  def generateInsertQuery(metadata: Map[String, EravanaMetadata], finalSchema: String, tbl: String, historyKeyword: String = ""): String = {
    val colStr = generateColStr(metadata, tbl)
    val rowHashStr = generateRowHashColStr(metadata, tbl)
    val batchKey = metadata(tbl).batchKey

    val (finalTbl, eventDt, eventDtCol, compareDt, compareDtCol, deleteFlagStr) = if (historyKeyword.nonEmpty) (tbl + historyKeyword, "event_date,", s"DATE(COALESCE($batchKey, CURRENT_TIMESTAMP)) AS event_date,", "compare_timestamp,", s"COALESCE($batchKey, CURRENT_TIMESTAMP) AS compare_timestamp,", "hist_final_delete_flag") else (tbl, "", "", "", "", "final_delete_flag")

    s"""
       |INSERT INTO $finalSchema.$finalTbl ($eventDt $compareDt $colStr, row_hash, is_deleted, etl_deleted_timestamp, etl_inserted_timestamp, etl_updated_timestamp)
       |SELECT
       |      $eventDtCol
       |      $compareDtCol
       |      $colStr
       |     ,$rowHashStr AS row_hash
       |     ,CASE WHEN op_type = 'delete' THEN true ELSE false END AS is_deleted
       |     ,CASE WHEN op_type = 'delete' THEN binlog_timestamp ELSE NULL END AS etl_deleted_timestamp
       |     ,COALESCE(etl_inserted_timestamp, CURRENT_TIMESTAMP) AS etl_inserted_timestamp
       |     ,CURRENT_TIMESTAMP AS etl_updated_timestamp
       |FROM $tbl WHERE $deleteFlagStr
     """.stripMargin
  }


  def generateTruncateQuery(stageSchema: String, tbl: String): String = {
    s"TRUNCATE TABLE $stageSchema.$tbl"
  }
}
