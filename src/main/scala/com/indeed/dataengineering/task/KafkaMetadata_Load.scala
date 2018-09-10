package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}
import com.datastax.driver.core._
import com.github.nscala_time.time.Imports.DateTime

import collection.JavaConverters._
import scala.collection.mutable


class KafkaMetadata_Load {
  def run(): Unit = {
    import spark.implicits._

    val sql = spark.sql _

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val Array(brokers, topics) = Array(conf("kafka.brokers"), conf("kafka.topic"))
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    log.info(s"Create Cassandra connector by passing host as ${conf("cassandra.host")}")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", conf("cassandra.host")))

    log.info("Connect to cassandra cluster")
    val cluster = Cluster.builder().addContactPoints(conf("cassandra.host").split(","): _*).build()
    val session = cluster.connect("metadata")

    val query = s"select topic, partition, offset from streaming_metadata where job = '$className';"
    log.info(s"Running Query in Cassandra to fetch partitions and offsets: $query")
    val res = session.execute(query).all.asScala.toArray

    val resMap = mutable.Map[String, mutable.Map[Int, Long]]()

    res.foreach { rec =>
      val topic = rec.getString("topic")
      val partition = rec.getInt("partition")
      val offset = rec.getLong("offset")
      val value = resMap.getOrElse(topic, mutable.Map[Int, Long]())
      resMap += topic -> (value + (partition -> Math.min(value.getOrElse(partition, offset), offset)))
    }

    val assignString = "{" + resMap.map { case (k, v) => s""""$k":[${v.keys.mkString(",")}]""" }.mkString(",") + "}"
    val offsetString = "{" + resMap.map { case (k, v) => s""""$k":{${v.map { case (p, o) => '"' + s"$p" + '"' + s":$o" }.mkString(",")}}""" }.mkString(",") + "}"

    log.info(s"Assign following topics and partitions: $assignString")
    log.info(s"Starting from the following offsets: $offsetString")

    log.info("Read Kafka streams")
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("assign", assignString)
      .option("startingOffsets", offsetString)
      .option("failOnDataLoss", conf.getOrElse("failOnDataLoss", "false"))
      .load()
    //.option("subscribe", topics)
    //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)
    //.option("assign", """{"maxwell":[4,7,1,9,3]}""")

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

        val cQuery2 =
          s"""
             |INSERT INTO metadata.streaming_metadata (job, db, tbl, topic, partition, offset)
             |VALUES ('$className', '${value.db}', '${value.tbl}', '${value.topic}', ${value.partition}, ${value.offset})
    			 """.stripMargin

        val cQuery3 =
          s"""
             |UPDATE stats.streaming_stats
             |SET inserted_records = inserted_records + 1, total_records_processed = total_records_processed + 1
             |WHERE job = '$className' AND db = '${value.db}' AND tbl = '${value.tbl}'
    			 """.stripMargin

        val cQuery4 =
          s"""
             |UPDATE stats.streaming_stats_by_hour
             |SET inserted_records = inserted_records + 1, total_records_processed = total_records_processed + 1
             |WHERE job = '$className' AND db = '${value.db}' AND tbl = '${value.tbl}' AND dt = '${DateTime.now.toString("yyyy-MM-dd")}' AND hr = ${DateTime.now.getHourOfDay}
    			 """.stripMargin

        connector.withSessionDo { session =>
          session.execute(cQuery1)
          session.execute(cQuery2)
          session.execute(cQuery3)
          session.execute(cQuery4)
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
