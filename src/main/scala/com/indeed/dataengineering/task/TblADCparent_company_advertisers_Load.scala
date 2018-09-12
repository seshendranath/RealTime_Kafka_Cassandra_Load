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


class TblADCparent_company_advertisers_Load {

  def run(rawData: DataFrame, connector: CassandraConnector): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblADCparent_company_advertisers"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCparent_company_advertisers = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCparent_company_advertisers.jsonSchema).as("value")).filter($"value.table" === "tblADCparent_company_advertisers").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCparent_company_advertisersWriter = new ForeachWriter[TblADCparent_company_advertisers] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCparent_company_advertisers): Unit = {

        val setClause = getSetClause(value.opType)

        val metaQueries = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val statQueries = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
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
        } else {
          s"""
             |DELETE FROM adcentraldb.tblADCparent_company_advertisers
             |WHERE parent_company_id = ${value.parent_company_id}
             |AND advertiser_id = ${value.advertiser_id}
           """.stripMargin
        }

        connector.withSessionDo { session =>
          val batchStatement1 = new BatchStatement
          val batchStatement2 = new BatchStatement(Type.UNLOGGED)
          batchStatement1.add(session.prepare(cQuery1).bind)
          metaQueries.foreach(q => batchStatement1.add(session.prepare(q).bind))
          statQueries.foreach(q => batchStatement2.add(session.prepare(q).bind))
          session.execute(batchStatement1)
          session.execute(batchStatement2)
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


