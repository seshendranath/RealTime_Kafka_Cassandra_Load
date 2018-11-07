package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{log => _, _}
import com.indeed.dataengineering.AnalyticsTaskApp._
import com.indeed.dataengineering.utilities.{Logging, SqlJDBC}
import com.indeed.dataengineering.utilities.SparkUtils._
import com.indeed.dataengineering.utilities.Utils._
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

    tables.foreach { tbl =>

      val transformString = Array("topic", "partition", "offset", "op_type", "binlog_timestamp", "binlog_position") ++ res(tbl).columns.map(c => transformations(c))
      log.info(s"Bool String for $tbl: ${transformString.mkString(",")}")

      log.info(s"Extracting Schema for table $tbl")
      val js = StructType(res(tbl).columns.map(c => StructField(c.name, postgresqlToSparkDataType(c.dataType))))
      log.info(s"Extracted Schema for $tbl: $js")

      val df = rawData.select($"topic", $"partition", $"offset", from_json($"value", MessageSchema.jsonSchema).as("value")).filter($"value.table" === tbl).select($"topic", $"partition", $"offset", $"value.type".as("op_type"), $"value.ts".as("binlog_timestamp"), $"value.position".as("binlog_position"), from_json($"value.data", js).as("data")).select($"topic", $"partition", $"offset", $"op_type", $"binlog_timestamp", $"binlog_position", $"data.*").selectExpr(transformString: _*).where("op_type IN ('insert', 'update', 'delete')")

      log.info(s"Starting Stream for table $tbl")
      val dfQuery = df.withColumn("dt", current_date).withColumn("hr", hour(current_timestamp)).writeStream.format(conf("targetFormat")).option("checkpointLocation",  s"${conf("s3Uri")}${conf("s3Bucket")}/${conf("baseLoc")}/checkpoint/$tbl/").option("path", s"${conf("s3Uri")}${conf("s3Bucket")}/${conf("baseLoc")}/$tbl").trigger(Trigger.ProcessingTime(s"${conf.getOrElse("runInterval", "5")} minutes")).partitionBy("dt", "hr").start()
      log.info(s"Query id for $tbl: ${dfQuery.id}")
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}
