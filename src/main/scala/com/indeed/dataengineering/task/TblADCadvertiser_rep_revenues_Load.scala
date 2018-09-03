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


class TblADCadvertiser_rep_revenues_Load {

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
    val tblADCadvertiser_rep_revenues = rawData.select(from_json($"value", TblADCadvertiser_rep_revenues.jsonSchema).as("value")).filter($"value.table" === "tblADCadvertiser_rep_revenues").select($"value.type".as("opType"), $"value.data.*")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCadvertiser_rep_revenuesWriter = new ForeachWriter[TblADCadvertiser_rep_revenues] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCadvertiser_rep_revenues): Unit = {

        // def intBool(i: Any): Any = if (i == null) null else if (i == 0) false else true

        val statsQuery = "UPDATE stats.kafka_stream_stats SET records_processed = records_processed + 1 WHERE db = 'adcentraldb' and tbl = 'tblADCadvertiser_rep_revenues'"

        if (value.opType == "insert" || value.opType == "update") {
          val cQuery1 =
            s"""
               |INSERT INTO adcentraldb.tblADCadvertiser_rep_revenues (activity_date,advertiser_id,relationship,user_id,revenue,revenue_jobsearch_millicents,revenue_resume_millicents,revenue_jobsearch_local,revenue_resume_local,currency,revenue_ss_millicents,revenue_hj_millicents,revenue_hjr_millicents,revenue_ss_local,revenue_hj_local,revenue_hjr_local,date_modified)
               |VALUES (
               | '${value.activity_date}'
               |,${value.advertiser_id}
               |,'${value.relationship}'
               |,${value.user_id.orNull}
               |,${value.revenue.orNull}
               |,${value.revenue_jobsearch_millicents.orNull}
               |,${value.revenue_resume_millicents.orNull}
               |,${value.revenue_jobsearch_local.orNull}
               |,${value.revenue_resume_local.orNull}
               |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''") + "'"}
               |,${value.revenue_ss_millicents.orNull}
               |,${value.revenue_hj_millicents.orNull}
               |,${value.revenue_hjr_millicents.orNull}
               |,${value.revenue_ss_local.orNull}
               |,${value.revenue_hj_local.orNull}
               |,${value.revenue_hjr_local.orNull}
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
               |DELETE FROM adcentraldb.tblADCadvertiser_rep_revenues
               |WHERE activity_date = ${value.activity_date}
               |AND advertiser_id = ${value.advertiser_id}
               |AND relationship = ${value.relationship}
             """.stripMargin
          connector.withSessionDo{session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tblADCadvertiser_rep_revenues.as[TblADCadvertiser_rep_revenues].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCadvertiser_rep_revenues.as[TblADCadvertiser_rep_revenues].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCadvertiser_rep_revenuesWriter).outputMode("append").start
    } else {
      tblADCadvertiser_rep_revenues.as[TblADCadvertiser_rep_revenues].writeStream.foreach(tblADCadvertiser_rep_revenuesWriter).outputMode("append").start
    }


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


