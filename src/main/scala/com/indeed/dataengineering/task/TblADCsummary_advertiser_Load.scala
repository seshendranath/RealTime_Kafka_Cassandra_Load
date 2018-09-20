package com.indeed.dataengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


// import com.datastax.driver.core.BatchStatement
// import com.datastax.driver.core.BatchStatement.Type
import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}
import com.indeed.dataengineering.utilities.Utils._
import org.apache.spark.sql.types.DateType


class TblADCsummary_advertiser_Load {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblADCsummary_advertiser"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCsummary_advertiser_tmp = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCsummary_advertiser.jsonSchema).as("value")).filter($"value.table" === "tblADCsummary_advertiser").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.ts".as("binlog_timestamp"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")
    val tblADCsummary_advertiser = tblADCsummary_advertiser_tmp.withColumn("activity_date", tblADCsummary_advertiser_tmp("binlog_timestamp").cast(DateType)).withColumn("is_deleted", when($"opType" === "delete", true).otherwise(false))

    log.info("Create ForeachWriter for Cassandra")
    val tblADCsummary_advertiserWriter = new ForeachWriter[TblADCsummary_advertiser] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCsummary_advertiser): Unit = {

        def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

        val setClause = getSetClause(value.opType)

        val metaQuery = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val (statQuery1, statQuery2) = getStatQueries(setClause, className, db, tbl)

        val cQuery1 =
          s"""
             |INSERT INTO adcentraldb.tblADCsummary_advertiser (activity_date,advertiser_id,account_id,masteraccount_id,company,date_created,active,monthly_budget,monthly_budget_local,expended_budget,expended_budget_local,lifetime_budget_local,expended_lifetime_budget_local,type,payment_method,billing_threshold,billing_threshold_local,industry,is_ad_agency,agency_discount,process_level,estimated_budget,estimated_budget_local,first_revenue_date,last_revenue_date,account_email,billing_zip,billing_state,billing_country,entity,vat_country,vat_number,vat_exempt_13b_number,agency_rep,sales_rep,service_rep,strategic_rep,sj_count,hj_count,hjr_count,daily_budget_hits_days,daily_budget_hits_ads,predicted_spend,predicted_spend_local,showhostedjobs,bid_managed,companypagestatus,sponjobindeedapply,locale,last_note_date,last_email_date,last_call_date,last_meeting_date,last_proposal_date,last_activity_date,parent_company_id,currency,nps_likely,nps_notusing_reason,commission_start_date,commission_last_date,employee_count,timestamp_created,date_modified,is_deleted,binlog_timestamp)
             |VALUES (
             | '${value.activity_date}'
             |,${value.advertiser_id}
             |,${value.account_id.orNull}
             |,${value.masteraccount_id.orNull}
             |,${if (value.company == null) null else "'" + value.company.replaceAll("'", "''")+ "'"}
             |,${if (value.date_created == null) null else "'" + value.date_created+ "'"}
             |,${value.active.orNull}
             |,${value.monthly_budget.orNull}
             |,${value.monthly_budget_local.orNull}
             |,${value.expended_budget.orNull}
             |,${value.expended_budget_local.orNull}
             |,${value.lifetime_budget_local.orNull}
             |,${value.expended_lifetime_budget_local.orNull}
             |,${if (value.`type` == null) null else "'" + value.`type`.replaceAll("'", "''")+ "'"}
             |,${if (value.payment_method == null) null else "'" + value.payment_method.replaceAll("'", "''")+ "'"}
             |,${value.billing_threshold.orNull}
             |,${value.billing_threshold_local.orNull}
             |,${if (value.industry == null) null else "'" + value.industry.replaceAll("'", "''")+ "'"}
             |,${intBool(value.is_ad_agency.orNull)}
             |,${value.agency_discount.orNull}
             |,${if (value.process_level == null) null else "'" + value.process_level.replaceAll("'", "''")+ "'"}
             |,${value.estimated_budget.orNull}
             |,${value.estimated_budget_local.orNull}
             |,${if (value.first_revenue_date == null) null else "'" + value.first_revenue_date+ "'"}
             |,${if (value.last_revenue_date == null) null else "'" + value.last_revenue_date+ "'"}
             |,${if (value.account_email == null) null else "'" + value.account_email.replaceAll("'", "''")+ "'"}
             |,${if (value.billing_zip == null) null else "'" + value.billing_zip.replaceAll("'", "''")+ "'"}
             |,${if (value.billing_state == null) null else "'" + value.billing_state.replaceAll("'", "''")+ "'"}
             |,${if (value.billing_country == null) null else "'" + value.billing_country.replaceAll("'", "''")+ "'"}
             |,${if (value.entity == null) null else "'" + value.entity.replaceAll("'", "''")+ "'"}
             |,${if (value.vat_country == null) null else "'" + value.vat_country.replaceAll("'", "''")+ "'"}
             |,${if (value.vat_number == null) null else "'" + value.vat_number.replaceAll("'", "''")+ "'"}
             |,${if (value.vat_exempt_13b_number == null) null else "'" + value.vat_exempt_13b_number.replaceAll("'", "''")+ "'"}
             |,${value.agency_rep.orNull}
             |,${value.sales_rep.orNull}
             |,${value.service_rep.orNull}
             |,${value.strategic_rep.orNull}
             |,${value.sj_count.orNull}
             |,${value.hj_count.orNull}
             |,${value.hjr_count.orNull}
             |,${value.daily_budget_hits_days.orNull}
             |,${value.daily_budget_hits_ads.orNull}
             |,${value.predicted_spend.orNull}
             |,${value.predicted_spend_local.orNull}
             |,${if (value.showHostedJobs == null) null else "'" + value.showHostedJobs.replaceAll("'", "''")+ "'"}
             |,${value.bid_managed.orNull}
             |,${if (value.companyPageStatus == null) null else "'" + value.companyPageStatus.replaceAll("'", "''")+ "'"}
             |,${if (value.sponJobIndeedApply == null) null else "'" + value.sponJobIndeedApply.replaceAll("'", "''")+ "'"}
             |,${if (value.locale == null) null else "'" + value.locale.replaceAll("'", "''")+ "'"}
             |,${if (value.last_note_date == null) null else "'" + value.last_note_date+ "'"}
             |,${if (value.last_email_date == null) null else "'" + value.last_email_date+ "'"}
             |,${if (value.last_call_date == null) null else "'" + value.last_call_date+ "'"}
             |,${if (value.last_meeting_date == null) null else "'" + value.last_meeting_date+ "'"}
             |,${if (value.last_proposal_date == null) null else "'" + value.last_proposal_date+ "'"}
             |,${if (value.last_activity_date == null) null else "'" + value.last_activity_date+ "'"}
             |,${value.parent_company_id.orNull}
             |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''")+ "'"}
             |,${value.nps_likely.orNull}
             |,${if (value.nps_notusing_reason == null) null else "'" + value.nps_notusing_reason.replaceAll("'", "''")+ "'"}
             |,${if (value.commission_start_date == null) null else "'" + value.commission_start_date+ "'"}
             |,${if (value.commission_last_date == null) null else "'" + value.commission_last_date+ "'"}
             |,${if (value.employee_count == null) null else "'" + value.employee_count.replaceAll("'", "''")+ "'"}
             |,${if (value.timestamp_created == null) null else "'" + value.timestamp_created+ "'"}
             |,${if (value.date_modified == null) null else "'" + value.date_modified+ "'"}
             |,${value.is_deleted}
             |,'${value.binlog_timestamp}'
             |)
           """.stripMargin

        if (executePlain) {
          connector.withSessionDo { session => session.execute(cQuery1) }
        } else if (executeMeta) {
          connector.withSessionDo { session =>
            /* val batchStatement1 = new BatchStatement
            batchStatement1.add(session.prepare(cQuery1).bind)
            batchStatement1.add(session.prepare(metaQuery).bind)
            session.execute(batchStatement1) */

            session.execute(cQuery1)
            session.execute(metaQuery)
          }
        } else {
          connector.withSessionDo { session =>
            /* val batchStatement1 = new BatchStatement
            batchStatement1.add(session.prepare(cQuery1).bind)
            batchStatement1.add(session.prepare(metaQuery).bind)
            session.execute(batchStatement1) */

            session.execute(cQuery1)
            session.execute(metaQuery)


            session.execute(statQuery1)
            session.execute(statQuery2)

          }
        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }


    if (conf.getOrElse("debug", "false") == "true") tblADCsummary_advertiser.as[TblADCsummary_advertiser].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCsummary_advertiser.as[TblADCsummary_advertiser].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCsummary_advertiserWriter).outputMode("append").start
    } else {
      tblADCsummary_advertiser.as[TblADCsummary_advertiser].writeStream.foreach(tblADCsummary_advertiserWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


