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


class TblADCparent_company_Load {

  def run(): Unit = {

    import spark.implicits._

    val checkpointDir = "s3a://indeed-data/dev/realtime/tmp/checkpoint/tblADCparent_company"

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
    val tblADCparent_company = rawData.select(from_json($"value", TblADCparent_company.jsonSchema).as("value")).filter($"value.table" === "tblADCparent_company").select($"value.type".as("opType"), $"value.data.*")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCparent_companyWriter = new ForeachWriter[TblADCparent_company] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCparent_company): Unit = {

        // def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

        val statsQuery = "UPDATE stats.kafka_stream_stats SET records_processed = records_processed + 1 WHERE db = 'adcentraldb' and tbl = 'tblADCparent_company'"

        if (value.opType == "insert" || value.opType == "update")
        {
          val cQuery1 =
            s"""
               |INSERT INTO adcentraldb.tblADCparent_company (id,company,sales_region,user_id,first_revenue_date,default_advertiser_id,prospecting_status,hq_city,hq_state,hq_zip,hq_country,duns,location_type,revenue,total_employees,franchise_operation_type,is_subsidiary,doing_business_as,exchange_symbol,exchange,USSIC,USSIC_description,NAICS,NAICS_description,parent_name,parent_duns,ultimate_domestic_parent_name,ultimate_domestic_parent_duns,ultimate_parent_name,ultimate_parent_duns,active_jobs,date_created,is_lead_eligible,lead_score,date_modified)
               |VALUES (
               | ${value.id}
               |,${if (value.company == null) null else "'" + value.company.replaceAll("'", "''")+ "'"}
               |,${if (value.sales_region == null) null else "'" + value.sales_region.replaceAll("'", "''")+ "'"}
               |,${value.user_id.orNull}
               |,${if (value.first_revenue_date == null) null else "'" + value.first_revenue_date+ "'"}
               |,${value.default_advertiser_id.orNull}
               |,${if (value.prospecting_status == null) null else "'" + value.prospecting_status.replaceAll("'", "''")+ "'"}
               |,${if (value.hq_city == null) null else "'" + value.hq_city.replaceAll("'", "''")+ "'"}
               |,${if (value.hq_state == null) null else "'" + value.hq_state.replaceAll("'", "''")+ "'"}
               |,${if (value.hq_zip == null) null else "'" + value.hq_zip.replaceAll("'", "''")+ "'"}
               |,${if (value.hq_country == null) null else "'" + value.hq_country.replaceAll("'", "''")+ "'"}
               |,${if (value.duns == null) null else "'" + value.duns.replaceAll("'", "''")+ "'"}
               |,${if (value.location_type == null) null else "'" + value.location_type.replaceAll("'", "''")+ "'"}
               |,${value.revenue.orNull}
               |,${value.total_employees.orNull}
               |,${if (value.franchise_operation_type == null) null else "'" + value.franchise_operation_type.replaceAll("'", "''")+ "'"}
               |,${value.is_subsidiary.orNull}
               |,${if (value.doing_business_as == null) null else "'" + value.doing_business_as.replaceAll("'", "''")+ "'"}
               |,${if (value.exchange_symbol == null) null else "'" + value.exchange_symbol.replaceAll("'", "''")+ "'"}
               |,${if (value.exchange == null) null else "'" + value.exchange.replaceAll("'", "''")+ "'"}
               |,${if (value.USSIC == null) null else "'" + value.USSIC.replaceAll("'", "''")+ "'"}
               |,${if (value.USSIC_description == null) null else "'" + value.USSIC_description.replaceAll("'", "''")+ "'"}
               |,${if (value.NAICS == null) null else "'" + value.NAICS.replaceAll("'", "''")+ "'"}
               |,${if (value.NAICS_description == null) null else "'" + value.NAICS_description.replaceAll("'", "''")+ "'"}
               |,${if (value.parent_name == null) null else "'" + value.parent_name.replaceAll("'", "''")+ "'"}
               |,${if (value.parent_duns == null) null else "'" + value.parent_duns.replaceAll("'", "''")+ "'"}
               |,${if (value.ultimate_domestic_parent_name == null) null else "'" + value.ultimate_domestic_parent_name.replaceAll("'", "''")+ "'"}
               |,${if (value.ultimate_domestic_parent_duns == null) null else "'" + value.ultimate_domestic_parent_duns.replaceAll("'", "''")+ "'"}
               |,${if (value.ultimate_parent_name == null) null else "'" + value.ultimate_parent_name.replaceAll("'", "''")+ "'"}
               |,${if (value.ultimate_parent_duns == null) null else "'" + value.ultimate_parent_duns.replaceAll("'", "''")+ "'"}
               |,${value.active_jobs.orNull}
               |,${if (value.date_created == null) null else "'" + value.date_created+ "'"}
               |,${value.is_lead_eligible.orNull}
               |,${value.lead_score.orNull}
               |,${if (value.date_modified == null) null else "'" + value.date_modified+ "'"}
               |)
             """.stripMargin
          connector.withSessionDo{session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
        else if (value.opType == "delete")
        {
          val cQuery1 =
            s"""
               |DELETE FROM adcentraldb.tblADCparent_company
               |WHERE id = ${value.id}
             """.stripMargin
          connector.withSessionDo{session =>
            session.execute(cQuery1)
            session.execute(statsQuery)
          }
        }
      }
      def close(errorOrNull: Throwable): Unit = {}
    }



    if (conf.getOrElse("debug", "false") == "true") tblADCparent_company.as[TblADCparent_company].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCparent_company.as[TblADCparent_company].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCparent_companyWriter).outputMode("append").start
    } else {
      tblADCparent_company.as[TblADCparent_company].writeStream.foreach(tblADCparent_companyWriter).outputMode("append").start
    }


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


