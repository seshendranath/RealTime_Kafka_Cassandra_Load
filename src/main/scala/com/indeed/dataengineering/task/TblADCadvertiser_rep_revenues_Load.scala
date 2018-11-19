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
import com.indeed.dataengineering.utilities.Logging
import org.apache.spark.sql.functions.{log => _, _}
import com.indeed.dataengineering.utilities.SqlUtils._


class TblADCadvertiser_rep_revenues_Load  extends Logging {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblADCadvertiser_rep_revenues"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCadvertiser_rep_revenues = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCadvertiser_rep_revenues.jsonSchema).as("value")).filter($"value.table" === "tblADCadvertiser_rep_revenues").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCadvertiser_rep_revenuesWriter = new ForeachWriter[TblADCadvertiser_rep_revenues] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCadvertiser_rep_revenues): Unit = {

        val setClause = getSetClause(value.opType)

        val metaQuery = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val (statQuery1, statQuery2) = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
          s"""
             |INSERT INTO adcentraldb.tblADCadvertiser_rep_revenues (offset,activity_date,advertiser_id,relationship,user_id,revenue,revenue_jobsearch_millicents,revenue_resume_millicents,revenue_jobsearch_local,revenue_resume_local,currency,revenue_ss_millicents,revenue_hj_millicents,revenue_hjr_millicents,revenue_ss_local,revenue_hj_local,revenue_hjr_local,date_modified)
             |VALUES (
             | ${value.offset}
             |,'${value.activity_date}'
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
        } else {
          s"""
             |DELETE FROM adcentraldb.tblADCadvertiser_rep_revenues
             |WHERE activity_date = '${value.activity_date}'
             |AND advertiser_id = ${value.advertiser_id}
             |AND relationship = '${value.relationship}'
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


