package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{log => _, _}
import com.indeed.dataengineering.AnalyticsTaskApp.spark
import com.indeed.dataengineering.LoadConfig.conf
import com.indeed.dataengineering.utilities.{Logging, SqlJDBC}
import com.indeed.dataengineering.utilities.SparkUtils._
import com.indeed.dataengineering.utilities.Utils._
import com.indeed.dataengineering.utilities.SqlUtils._
import com.indeed.dataengineering.models._
import org.apache.spark.sql.streaming.Trigger


class Kafka_S3_Load extends Logging {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {
    import spark.implicits._

    log.info("Creating Postgresql connection")
    val postgresql = new SqlJDBC("postgresql", conf("metadata.url"), conf("metadata.user"), conf("metadata.password"))

    val tables = conf("whitelistedTables").split(",").toSet

    log.info(s"Building metadata for whitelisted tables $tables")
    val res = buildMetadata(postgresql, tables)

    val pkPrefix = "event_old_pk_"
    val pkChangeColName = "event_pk_changed"

    tables.foreach { tbl =>

      val pk = res(tbl).primaryKey.map(_.name)
      val pkCols = pk.map(c => s"$pkPrefix$c") :+ pkChangeColName
      val pkArray = pk.map(c => s"COALESCE(old.$c, data.$c) AS $pkPrefix$c") :+ ("CASE WHEN " + pk.map(c => s"COALESCE(old.$c, data.$c) = data.$c").mkString(" AND ") + s" THEN FALSE ELSE TRUE END AS $pkChangeColName")

      val transformString = Array("topic", "partition", "offset", "op_type", "binlog_timestamp", "SPLIT(binlog_position, ':')[0] AS binlog_file", "CAST(SPLIT(binlog_position, ':')[1] AS BIGINT) AS binlog_position") ++ res(tbl).columns.map(c => transformations(c)) ++ pkCols
      log.info(s"Bool String for $tbl: ${transformString.mkString(",")}")

      log.info(s"Extracting Schema for table $tbl")
      val js = StructType(res(tbl).columns.map(c => StructField(columnNameTransformations(c.name), postgresqlToSparkDataType(c.dataType))))
      log.info(s"Extracted Schema for $tbl: $js")

      log.info(s"Extracting Old Primary Key Schema for table $tbl")
      val oldJs = StructType(res(tbl).primaryKey.map(c => StructField(c.name, postgresqlToSparkDataType(c.dataType))))
      log.info(s"Extracted Old Primary Key Schema for $tbl: $oldJs")

      val selectCols = Array("topic", "partition", "offset", "op_type", "binlog_timestamp", "binlog_position", "data.*") ++ pkArray
      val df = rawData.select($"topic", $"partition", $"offset", from_json($"value", MessageSchema.jsonSchema).as("value")).filter($"value.table" === tbl).select($"topic", $"partition", $"offset", $"value.type".as("op_type"), $"value.ts".as("binlog_timestamp"), $"value.position".as("binlog_position"), from_json($"value.data", js).as("data"), from_json($"value.old", oldJs).as("old")).selectExpr(selectCols: _*).selectExpr(transformString: _*).where("op_type IN ('insert', 'update', 'delete')").withColumn("dt", current_date).withColumn("hr", hour(current_timestamp))

      val finalCols = Array("topic", "partition", "offset", "op_type", "binlog_timestamp", "binlog_file", "binlog_position", "dt", "hr") ++ res(tbl).columns.map(_.name)
      log.info(s"Selecting Final Cols for the $tbl: ${finalCols.mkString(",")}")
      val df1 = df.selectExpr(finalCols: _*)
      val df2 = df.where(s"$pkChangeColName = TRUE").selectExpr((df.columns.toSet diff pk.toSet).map(c => if (c.startsWith(pkPrefix)) s"$c AS ${c.stripPrefix(pkPrefix)}" else if (c == "op_type") "'delete' AS op_type" else c).toArray: _*).drop(pkChangeColName).selectExpr(finalCols: _*)
      val finalDf = df1.union(df2)

      log.info(s"Starting Stream for table $tbl")
      val dfQuery = finalDf.writeStream.format(conf("targetFormat")).option("checkpointLocation", s"${conf("s3Uri")}${conf("s3Bucket")}/${conf("baseLoc")}/checkpoint/$tbl/").option("path", s"${conf("s3Uri")}${conf("s3Bucket")}/${conf("baseLoc")}/$tbl").trigger(Trigger.ProcessingTime(s"${conf.getOrElse("runInterval", "5")} minutes")).partitionBy("dt", "hr").start()
      log.info(s"Query id for $tbl: ${dfQuery.id}")
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}
