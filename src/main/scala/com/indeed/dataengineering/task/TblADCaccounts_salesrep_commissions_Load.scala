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


class TblADCaccounts_salesrep_commissions_Load {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblADCaccounts_salesrep_commissions"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCaccounts_salesrep_commissions = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCaccounts_salesrep_commissions.jsonSchema).as("value")).filter($"value.table" === "tblADCaccounts_salesrep_commissions").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")

    log.info("Create ForeachWriter for Cassandra")
    val tblADCaccounts_salesrep_commissionsWriter = new ForeachWriter[TblADCaccounts_salesrep_commissions] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCaccounts_salesrep_commissions): Unit = {

        val setClause = getSetClause(value.opType)

        val metaQuery = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val (statQuery1, statQuery2) = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
          s"""
             |INSERT INTO adcentraldb.tblADCaccounts_salesrep_commissions (offset,date,advertiser_id,salesrep_id,revenue_jobsearch_millicents,revenue_dradis_lifetime_millicents,revenue_dradis_recurring_millicents,revenue_resume_millicents,revenue_ineligible_millicents,revenue_jobsearch_local,revenue_dradis_lifetime_local,revenue_dradis_recurring_local,revenue_resume_local,revenue_ineligible_local,discount_local,discount_forward_local,discounted_revenue_local,commission_rate,commission_amount_local,commission_amount_millicents,newrevenue_jobsearch_millicents,newrevenue_dradis_lifetime_millicents,newrevenue_dradis_recurring_millicents,newrevenue_resume_millicents,currency,date_modified)
             |VALUES (
             | ${value.offset}
             |,'${value.date}'
             |,${value.advertiser_id}
             |,${value.salesrep_id.orNull}
             |,${value.revenue_jobsearch_millicents.orNull}
             |,${value.revenue_dradis_lifetime_millicents.orNull}
             |,${value.revenue_dradis_recurring_millicents.orNull}
             |,${value.revenue_resume_millicents.orNull}
             |,${value.revenue_ineligible_millicents.orNull}
             |,${value.revenue_jobsearch_local.orNull}
             |,${value.revenue_dradis_lifetime_local.orNull}
             |,${value.revenue_dradis_recurring_local.orNull}
             |,${value.revenue_resume_local.orNull}
             |,${value.revenue_ineligible_local.orNull}
             |,${value.discount_local.orNull}
             |,${value.discount_forward_local.orNull}
             |,${value.discounted_revenue_local.orNull}
             |,${value.commission_rate.orNull}
             |,${value.commission_amount_local.orNull}
             |,${value.commission_amount_millicents.orNull}
             |,${value.newrevenue_jobsearch_millicents.orNull}
             |,${value.newrevenue_dradis_lifetime_millicents.orNull}
             |,${value.newrevenue_dradis_recurring_millicents.orNull}
             |,${value.newrevenue_resume_millicents.orNull}
             |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''") + "'"}
             |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
             |)
           """.stripMargin
        } else {
          s"""
             |DELETE FROM adcentraldb.tblADCaccounts_salesrep_commissions
             |WHERE date = ${value.date}
             |AND advertiser_id = ${value.advertiser_id}
           """.stripMargin
        }

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


    if (conf.getOrElse("debug", "false") == "true") tblADCaccounts_salesrep_commissions.as[TblADCaccounts_salesrep_commissions].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCaccounts_salesrep_commissions.as[TblADCaccounts_salesrep_commissions].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCaccounts_salesrep_commissionsWriter).outputMode("append").start
    } else {
      tblADCaccounts_salesrep_commissions.as[TblADCaccounts_salesrep_commissions].writeStream.foreach(tblADCaccounts_salesrep_commissionsWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


