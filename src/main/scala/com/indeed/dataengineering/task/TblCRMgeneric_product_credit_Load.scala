package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}


class TblCRMgeneric_product_credit_Load {

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
    val tblCRMgeneric_product_credit = rawData.select(from_json($"value", TblCRMgeneric_product_credit.jsonSchema).as("value")).filter($"value.table" === "tblCRMgeneric_product_credit").select($"value.type".as("opType"), $"value.data.*")


    log.info("Create ForeachWriter for Cassandra")
    val tblCRMgeneric_product_creditWriter = new ForeachWriter[TblCRMgeneric_product_credit] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblCRMgeneric_product_credit): Unit = {

        def intBool(i: Any): Any = if (i == null) null else if (i == 0) false else true

        val statsQuery = "UPDATE stats.kafka_stream_stats SET records_processed = records_processed + 1 WHERE db = 'adcentraldb' and tbl = 'tblCRMgeneric_product_credit'"

        if (value.opType == "insert" || value.opType == "update") {
          val cQuery1 =
            s"""
               |INSERT INTO adcentraldb.tblCRMgeneric_product_credit (id,activity_date,advertiser_id,relationship,user_id,product_id,revenue_generic_product_millicents,revenue_generic_product_local,currency,invoice_request_id,rejected,date_added,date_modified)
               |VALUES (
               | ${value.id}
               |,${if (value.activity_date == null) null else "'" + value.activity_date + "'"}
               |,${value.advertiser_id.orNull}
               |,${if (value.relationship == null) null else "'" + value.relationship.replaceAll("'", "''") + "'"}
               |,${value.user_id.orNull}
               |,${value.product_id.orNull}
               |,${value.revenue_generic_product_millicents.orNull}
               |,${value.revenue_generic_product_local.orNull}
               |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''") + "'"}
               |,${value.invoice_request_id.orNull}
               |,${intBool(value.rejected.orNull)}
               |,${if (value.date_added == null) null else "'" + value.date_added + "'"}
               |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
               |)
             """.stripMargin
          connector.withSessionDo { session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
        else if (value.opType == "delete") {
          val cQuery1 =
            s"""
               |DELETE FROM adcentraldb.tblCRMgeneric_product_credit
               |WHERE id = ${value.id}
             """.stripMargin
          connector.withSessionDo { session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tblCRMgeneric_product_credit.as[TblCRMgeneric_product_credit].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    log.info("Write Streams to Cassandra Table")
    tblCRMgeneric_product_credit.as[TblCRMgeneric_product_credit].writeStream.foreach(tblCRMgeneric_product_creditWriter).outputMode("append").start


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


