package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */

import scala.collection.mutable
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{log => _, _}
import com.indeed.dataengineering.AnalyticsTaskApp._
import com.indeed.dataengineering.utilities.PostgreSql
import com.indeed.dataengineering.utilities.Utils._
import com.indeed.dataengineering.models._
import org.apache.spark.sql.streaming.Trigger


class Kafka_S3_Load {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {
    import spark.implicits._

    log.info("Creating Postgresql connection")
    val postgresql = new PostgreSql(conf("metadata.url"), conf("metadata.user"), conf("metadata.password"))

    val tables = conf("whitelistedTables").split(",").toSet

    log.info(s"Building metadata for whitelisted tables $tables")
    val res = buildMetadata(postgresql, tables)

    tables.foreach { tbl =>

      val boolString = Array("topic", "partition", "offset", "opType") ++ res(tbl).map(c => if (c._2 == "BOOLEAN") s"CAST(${c._1} AS Boolean) AS ${c._1}" else c._1)

      log.info(s"Extracting Schema for table $tbl")
      val js = StructType(res(tbl).map(c => StructField(c._1, postgresqlToSparkDataType(c._2))))
      val df = rawData.select($"topic", $"partition", $"offset", from_json($"value", MessageSchema.jsonSchema).as("value")).filter($"value.table" === tbl).select($"topic", $"partition", $"offset", $"value.type".as("opType"), from_json($"value.data", js).as("data")).select($"topic", $"partition", $"offset", $"opType", $"data.*").selectExpr(boolString: _*).where("opType IN ('insert', 'update', 'delete')")

      log.info(s"Starting Stream for table $tbl")
      val dfQuery = df.withColumn("dt", current_date).withColumn("hr", hour(current_timestamp)).writeStream.format(conf("targetFormat")).option("checkpointLocation", conf("baseLoc") + s"/checkpoint/$tbl/").option("path", conf("baseLoc") + s"/$tbl").trigger(Trigger.ProcessingTime(s"${conf("runInterval")} minutes")).partitionBy("dt", "hr").start()
      log.info(s"Query id for $tbl: ${dfQuery.id}")
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

  def getDatasetId(postgresql: PostgreSql, tbl: String): Int = {
    val query = s"select dataset_id from eravana.dataset where name='$tbl'"

    log.info(s"Running Query: $query")
    val rs = postgresql.executeQuery(query)

    var dataset_id = -1
    while (rs.next()) dataset_id = rs.getInt("dataset_id")

    log.info(s"Query $query completed successfully")
    dataset_id
  }


  def getColumns(postgresql: PostgreSql, dataset_id: Int): Array[(String, String)] = {
    val query = s"select name, data_type from eravana.dataset_column where dataset_id = $dataset_id order by ordinal_position"

    log.info(s"Running Query: $query")
    val rs = postgresql.executeQuery(query)

    val result = mutable.ArrayBuffer[(String, String)]()

    while (rs.next()) result += ((rs.getString("name"), rs.getString("data_type")))

    log.info(s"Query $query completed successfully")
    result.toArray
  }


  def buildMetadata(postgreSql: PostgreSql, tables: Set[String]): Map[String, Array[(String, String)]] = {
    val result = mutable.Map[String, Array[(String, String)]]()

    tables.foreach { tbl =>
      val dataset_id = getDatasetId(postgreSql, tbl)
      val columns = getColumns(postgreSql, dataset_id)
      result += tbl -> columns
    }

    result.toMap
  }

}
