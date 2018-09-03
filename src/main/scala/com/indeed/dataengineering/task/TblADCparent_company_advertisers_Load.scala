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


class TblADCparent_company_advertisers_Load {

  def run(): Unit = {

    import spark.implicits._

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
    val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCparent_company_advertisers = rawData.select(from_json($"value", TblADCparent_company_advertisers.jsonSchema).as("value")).filter($"value.table" === "tblADCparent_company_advertisers").select($"value.type".as("opType"), $"value.data.*")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCparent_company_advertisersWriter = new ForeachWriter[TblADCparent_company_advertisers] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCparent_company_advertisers): Unit = {

        //  def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

        val statsQuery = "UPDATE stats.kafka_stream_stats SET records_processed = records_processed + 1 WHERE db = 'adcentraldb' and tbl = 'tblADCparent_company_advertisers'"

        if (value.opType == "insert" || value.opType == "update") {
          val cQuery1 =
            s"""
               |INSERT INTO adcentraldb.tblADCparent_company_advertisers (parent_company_id,advertiser_id,date_created,assignment_method,assigned_by,date_modified)
               |VALUES (
               | ${value.parent_company_id}
               |,${value.advertiser_id}
               |,${if (value.date_created == null) null else "'" + value.date_created + "'"}
               |,${if (value.assignment_method == null) null else "'" + value.assignment_method.replaceAll("'", "''") + "'"}
               |,${value.assigned_by.orNull}
               |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
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
               |DELETE FROM adcentraldb.tblADCparent_company_advertisers
               |WHERE parent_company_id = ${value.parent_company_id}
               |AND advertiser_id = ${value.advertiser_id}
             """.stripMargin
          connector.withSessionDo{session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tblADCparent_company_advertisers.as[TblADCparent_company_advertisers].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCparent_company_advertisers.as[TblADCparent_company_advertisers].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCparent_company_advertisersWriter).outputMode("append").start
    } else {
      tblADCparent_company_advertisers.as[TblADCparent_company_advertisers].writeStream.foreach(tblADCparent_company_advertisersWriter).outputMode("append").start
    }


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


