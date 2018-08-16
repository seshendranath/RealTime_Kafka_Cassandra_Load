package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}


class TblADScurrency_rates_Load {

  def run(): Unit = {

    import spark.implicits._

    val Array(brokers, topics) = Array(conf("kafka.brokers"), conf("kafka.topic"))
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    log.info(s"Create Cassandra connector by passing host as ${conf("cassandra.host")}")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", conf("cassandra.host")))

    log.info("Read Kafka streams")
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()
    //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)

    log.info("Extract value and map from Kafka consumer records")
    val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADScurrency_rates = rawData.select(from_json($"value", TblADScurrency_rates.jsonSchema).as("value")).filter($"value.table" === "tblADScurrency_rates").select($"value.type".as("opType"), $"value.data.*")


    log.info("Create ForeachWriter for Cassandra")
    val tblADScurrency_ratesWriter = new ForeachWriter[TblADScurrency_rates] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADScurrency_rates): Unit = {

        // def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

        if (value.opType == "insert" || value.opType == "update") {
          val cQuery1 =
            s"""
               |INSERT INTO adsystemdb.tblADScurrency_rates (activity_date,from_currency,to_currency,exchange_rate,buy_rate,sell_rate,source,date_modified)
               |VALUES (
               | '${value.activity_date}'
               |,'${value.from_currency}'
               |,'${value.to_currency}'
               |,${value.exchange_rate.orNull}
               |,${value.buy_rate.orNull}
               |,${value.sell_rate.orNull}
               |,${if (value.source == null) null else "'" + value.source.replaceAll("'", "''") + "'"}
               |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
               |)
             """.stripMargin
          connector.withSessionDo { session => session.execute(cQuery1) }
        }
        else if (value.opType == "delete") {
          val cQuery1 =
            s"""
               |DELETE FROM adsystemdb.tblADScurrency_rates
               |WHERE activity_date = ${value.activity_date}
               |AND to_currency = ${value.to_currency}
               |AND from_currency = ${value.from_currency}
             """.stripMargin
          connector.withSessionDo { session => session.execute(cQuery1) }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tblADScurrency_rates.as[TblADScurrency_rates].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    log.info("Write Streams to Cassandra Table")
    tblADScurrency_rates.as[TblADScurrency_rates].writeStream.foreach(tblADScurrency_ratesWriter).outputMode("append").start


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


