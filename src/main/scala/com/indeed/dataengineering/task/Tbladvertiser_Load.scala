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
import com.indeed.dataengineering.utilities.Utils._


class Tbladvertiser_Load {

  def run(rawData: DataFrame, connector: CassandraConnector): Unit = {

    import spark.implicits._

    val db = "adsystemdb"
    val tbl = "tbladvertiser"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tbladvertiser = rawData.select($"topic", $"partition", $"offset", from_json($"value", Tbladvertiser.jsonSchema).as("value")).filter($"value.table" === "tbladvertiser").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tbladvertiserWriter = new ForeachWriter[Tbladvertiser] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: Tbladvertiser): Unit = {

        def intBool(i: Any): Any = if (i == null) null else if (i == 0) false else true

        val setClause = getSetClause(value.opType)

        val metaQueries = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val statQueries = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
          s"""
             |INSERT INTO adsystemdb.tbladvertiser (id,account_id,company,contact,url,address1,address2,city,state,zip,phone,phone_type,verified_phone,verified_phone_extension,uuidstring,date_created,active,ip_address,referral_id,monthly_budget,expended_budget,type,advertiser_number,show_conversions,billing_threshold,payment_method,industry,agency_discount,is_ad_agency,process_level,estimated_budget,first_revenue_date,last_revenue_date,first_revenue_override,terms,currency,monthly_budget_local,expended_budget_local,billing_threshold_local,estimated_budget_local,employee_count,last_updated)
             |VALUES (
             | ${value.id}
             |,${value.account_id.orNull}
             |,${if (value.company == null) null else "'" + value.company.replaceAll("'", "''") + "'"}
             |,${if (value.contact == null) null else "'" + value.contact.replaceAll("'", "''") + "'"}
             |,${if (value.url == null) null else "'" + value.url.replaceAll("'", "''") + "'"}
             |,${if (value.address1 == null) null else "'" + value.address1.replaceAll("'", "''") + "'"}
             |,${if (value.address2 == null) null else "'" + value.address2.replaceAll("'", "''") + "'"}
             |,${if (value.city == null) null else "'" + value.city.replaceAll("'", "''") + "'"}
             |,${if (value.state == null) null else "'" + value.state.replaceAll("'", "''") + "'"}
             |,${if (value.zip == null) null else "'" + value.zip.replaceAll("'", "''") + "'"}
             |,${if (value.phone == null) null else "'" + value.phone.replaceAll("'", "''") + "'"}
             |,${if (value.phone_type == null) null else "'" + value.phone_type.replaceAll("'", "''") + "'"}
             |,${if (value.verified_phone == null) null else "'" + value.verified_phone.replaceAll("'", "''") + "'"}
             |,${if (value.verified_phone_extension == null) null else "'" + value.verified_phone_extension.replaceAll("'", "''") + "'"}
             |,${if (value.uuidstring == null) null else "'" + value.uuidstring.replaceAll("'", "''") + "'"}
             |,${if (value.date_created == null) null else "'" + value.date_created + "'"}
             |,${value.active.orNull}
             |,${if (value.ip_address == null) null else "'" + value.ip_address.replaceAll("'", "''") + "'"}
             |,${value.referral_id.orNull}
             |,${value.monthly_budget.orNull}
             |,${value.expended_budget.orNull}
             |,${if (value.`type` == null) null else "'" + value.`type`.replaceAll("'", "''") + "'"}
             |,${if (value.advertiser_number == null) null else "'" + value.advertiser_number.replaceAll("'", "''") + "'"}
             |,${value.show_conversions.orNull}
             |,${value.billing_threshold.orNull}
             |,${if (value.payment_method == null) null else "'" + value.payment_method.replaceAll("'", "''") + "'"}
             |,${if (value.industry == null) null else "'" + value.industry.replaceAll("'", "''") + "'"}
             |,${value.agency_discount.orNull}
             |,${intBool(value.is_ad_agency.orNull)}
             |,${if (value.process_level == null) null else "'" + value.process_level.replaceAll("'", "''") + "'"}
             |,${value.estimated_budget.orNull}
             |,${if (value.first_revenue_date == null) null else "'" + value.first_revenue_date + "'"}
             |,${if (value.last_revenue_date == null) null else "'" + value.last_revenue_date + "'"}
             |,${if (value.first_revenue_override == null) null else "'" + value.first_revenue_override + "'"}
             |,${if (value.terms == null) null else "'" + value.terms.replaceAll("'", "''") + "'"}
             |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''") + "'"}
             |,${value.monthly_budget_local.orNull}
             |,${value.expended_budget_local.orNull}
             |,${value.billing_threshold_local.orNull}
             |,${value.estimated_budget_local.orNull}
             |,${if (value.employee_count == null) null else "'" + value.employee_count.replaceAll("'", "''") + "'"}
             |,${if (value.last_updated == null) null else "'" + value.last_updated + "'"}
             |)
           """.stripMargin
        } else {
          s"""
             |DELETE FROM adsystemdb.tbladvertiser
             |WHERE id = ${value.id}
           """.stripMargin
        }

        if (executePlain) {
          connector.withSessionDo { session => session.execute(cQuery1) }
        } else if (executeMeta) {
          connector.withSessionDo { session =>
            val batchStatement1 = new BatchStatement
            batchStatement1.add(session.prepare(cQuery1).bind)
            metaQueries.foreach(q => batchStatement1.add(session.prepare(q).bind))
            session.execute(batchStatement1)
          }
        } else {
          connector.withSessionDo { session =>
            val batchStatement1 = new BatchStatement
            batchStatement1.add(session.prepare(cQuery1).bind)
            metaQueries.foreach(q => batchStatement1.add(session.prepare(q).bind))
            session.execute(batchStatement1)

            val batchStatement2 = new BatchStatement(Type.UNLOGGED)
            statQueries.foreach(q => batchStatement2.add(session.prepare(q).bind))
            session.execute(batchStatement2)
          }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tbladvertiser.as[Tbladvertiser].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tbladvertiser.as[Tbladvertiser].writeStream.option("checkpointLocation", checkpointDir).foreach(tbladvertiserWriter).outputMode("append").start
    } else {
      tbladvertiser.as[Tbladvertiser].writeStream.foreach(tbladvertiserWriter).outputMode("append").start
    }


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


