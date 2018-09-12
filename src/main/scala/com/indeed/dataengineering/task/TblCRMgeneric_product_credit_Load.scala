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


class TblCRMgeneric_product_credit_Load {

  def run(rawData: DataFrame, connector: CassandraConnector): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblCRMgeneric_product_credit"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    log.info("Map extracted kafka consumer records to Case Class")
    val tblCRMgeneric_product_credit = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblCRMgeneric_product_credit.jsonSchema).as("value")).filter($"value.table" === "tblCRMgeneric_product_credit").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tblCRMgeneric_product_creditWriter = new ForeachWriter[TblCRMgeneric_product_credit] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblCRMgeneric_product_credit): Unit = {

        def intBool(i: Any): Any = if (i == null) null else if (i == 0) false else true

        val setClause = getSetClause(value.opType)

        val metaQueries = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val statQueries = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
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
        } else {
          s"""
             |DELETE FROM adcentraldb.tblCRMgeneric_product_credit
             |WHERE id = ${value.id}
           """.stripMargin

        }

        connector.withSessionDo { session =>

          if (conf.getOrElse("executePlain", "false").toBoolean) {
            session.execute(cQuery1)
          } else if (conf.getOrElse("executeMeta", "false").toBoolean) {
            val batchStatement1 = new BatchStatement
            batchStatement1.add(session.prepare(cQuery1).bind)
            metaQueries.foreach(q => batchStatement1.add(session.prepare(q).bind))
            session.execute(batchStatement1)
          } else {
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


    if (conf.getOrElse("debug", "false") == "true") tblCRMgeneric_product_credit.as[TblCRMgeneric_product_credit].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblCRMgeneric_product_credit.as[TblCRMgeneric_product_credit].writeStream.option("checkpointLocation", checkpointDir).foreach(tblCRMgeneric_product_creditWriter).outputMode("append").start
    } else {
      tblCRMgeneric_product_credit.as[TblCRMgeneric_product_credit].writeStream.foreach(tblCRMgeneric_product_creditWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


