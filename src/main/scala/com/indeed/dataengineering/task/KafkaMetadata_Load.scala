package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.BatchStatement.Type
import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}
import com.indeed.dataengineering.utilities.Utils.{getMetaQueries, getStatQueries}


class KafkaMetadata_Load {

  def run(rawData: DataFrame, connector: CassandraConnector): Unit = {

    import spark.implicits._

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val sql = spark.sql _

    val massagedData = rawData.select($"topic", $"partition", $"offset", $"kafka_timestamp", from_json($"value", KafkaMetadata.jsonSchema).as("value")).select($"topic", $"partition", $"offset", $"kafka_timestamp", $"value.database", $"value.table", $"value.position", $"value.ts", $"value.primary_key", $"value.data.*")
    massagedData.createOrReplaceTempView("massagedData")

    val kafkaMetadataWriter = new ForeachWriter[KafkaMetadata] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: KafkaMetadata): Unit = {
        val cQuery1 =
          s"""
             |INSERT INTO metadata.kafka_metadata (db, tbl, tbl_date_modified, topic, partition, offset, primary_key, binlog_position, kafka_timestamp, binlog_timestamp, meta_last_updated)
             |VALUES ('${value.db}', '${value.tbl}', '${value.tbl_date_modified}', '${value.topic}', ${value.partition}, ${value.offset}, '${value.primary_key}', '${value.binlog_position}', '${value.kafka_timestamp}', '${value.binlog_timestamp}', toTimestamp(now()))
           """.stripMargin

        val setClause = "inserted_records = inserted_records + 1"

        val metaQueries = getMetaQueries(className, value.db, value.tbl, value.topic, value.partition, value.offset)

        val statQueries = getStatQueries(setClause, className, value.db, value.tbl)

        connector.withSessionDo { session =>
          val batchStatement1 = new BatchStatement
          val batchStatement2 = new BatchStatement(Type.UNLOGGED)
          batchStatement1.add(session.prepare(cQuery1).bind)
          metaQueries.foreach(q => batchStatement1.add(session.prepare(q).bind))
          statQueries.foreach(q => batchStatement2.add(session.prepare(q).bind))
          session.execute(batchStatement1)
          session.execute(batchStatement2)
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }

    val kafkaMetadataQuery =
      """
        	|SELECT
        	|		 database AS db
        	|		,table AS tbl
        	|		,CASE WHEN date_modified IS NOT NULL THEN date_modified ELSE last_updated END AS tbl_date_modified
        	|		,topic
        	|		,partition
        	|		,offset
        	|		,primary_key
        	|		,position AS binlog_position
        	|		,ts AS binlog_timestamp
        	|		,kafka_timestamp
        	|FROM massagedData
      	""".stripMargin

    val df = sql(kafkaMetadataQuery).where("tbl_date_modified IS NOT NULL")

    log.info("Write Streams to Cassandra Metadata Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      df.as[KafkaMetadata].writeStream.option("checkpointLocation", checkpointDir).foreach(kafkaMetadataWriter).outputMode("append").start
    } else {
      df.as[KafkaMetadata].writeStream.foreach(kafkaMetadataWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }
}
