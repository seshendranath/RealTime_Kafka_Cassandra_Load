package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
// import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{log => _, _}


class TblADCquota_Load {

  def run(): Unit = {

    import spark.implicits._

    val checkpointDir = "s3a://indeed-data/dev/realtime/tmp/checkpoint/tblADCquota"

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
    val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCquota = rawData.select(from_json($"value", TblADCquota.jsonSchema).as("value")).filter($"value.table" === "tblADCquota").select($"value.type".as("opType"), $"value.data.*")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCquotaWriter = new ForeachWriter[TblADCquota] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCquota): Unit = {

        //  def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

        val statsQuery = "UPDATE stats.kafka_stream_stats SET records_processed = records_processed + 1 WHERE db = 'adcentraldb' and tbl = 'tblADCquota'"

        if (value.opType == "insert" || value.opType == "update") {
          val cQuery1 =
            s"""
               |INSERT INTO adcentraldb.tblADCquota (year,month,user_id,quota_type,quota,quota_local,currency,date_added,date_modified,edited_by)
               |VALUES (
               | ${value.year}
               |,${value.month}
               |,${value.user_id}
               |,'${value.quota_type}'
               |,${value.quota.orNull}
               |,${value.quota_local.orNull}
               |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''") + "'"}
               |,${if (value.date_added == null) null else "'" + value.date_added + "'"}
               |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
               |,${value.edited_by.orNull}
               |)
             """.stripMargin
          connector.withSessionDo{session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
        else if (value.opType == "delete") {
          val cQuery1 =
            s"""
               |DELETE FROM adcentraldb.tblADCquota
               |WHERE year = ${value.year}
               |AND month = ${value.month}
               |AND user_id = ${value.user_id}
               |AND quota_type = ${value.quota_type}
             """.stripMargin
          connector.withSessionDo{session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tblADCquota.as[TblADCquota].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCquota.as[TblADCquota].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCquotaWriter).outputMode("append").start
    } else {
      tblADCquota.as[TblADCquota].writeStream.foreach(tblADCquotaWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


