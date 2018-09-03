package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}


class KafkaMetadata_Load {
  def run(): Unit = {
    import spark.implicits._

    val sql = spark.sql _

    val checkpointDir = conf("checkpointBaseLoc") + this.getClass.getSimpleName

    val Array(brokers, topics) = Array(conf("kafka.brokers"), conf("kafka.topic"))
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    log.info(s"Create Cassandra connector by passing host as ${conf("cassandra.host")}")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", conf("cassandra.host")))

    log.info("Read Kafka streams")
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics).option("failOnDataLoss", "false")
      .load()
    //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)

    log.info("Extract value and map from Kafka consumer records")
    val rawData = kafkaStream.selectExpr("topic", "partition", "offset", "timestamp AS kafka_timestamp", "CAST(value AS STRING)")

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

        connector.withSessionDo { session => session.execute(cQuery1) }
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

    log.info("Write Streams to Cassandra Metadata Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      sql(kafkaMetadataQuery).as[KafkaMetadata].writeStream.option("checkpointLocation", checkpointDir).foreach(kafkaMetadataWriter).outputMode("append").start
    } else {
      sql(kafkaMetadataQuery).as[KafkaMetadata].writeStream.foreach(kafkaMetadataWriter).outputMode("append").start
    }


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }
}
