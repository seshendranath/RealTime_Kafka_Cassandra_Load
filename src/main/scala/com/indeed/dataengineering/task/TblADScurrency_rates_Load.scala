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


class TblADScurrency_rates_Load {

  def run(rawData: DataFrame, connector: CassandraConnector): Unit = {

    import spark.implicits._

    val db = "adsystemdb"
    val tbl = "tblADScurrency_rates"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADScurrency_rates = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADScurrency_rates.jsonSchema).as("value")).filter($"value.table" === "tblADScurrency_rates").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tblADScurrency_ratesWriter = new ForeachWriter[TblADScurrency_rates] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADScurrency_rates): Unit = {

        val setClause = getSetClause(value.opType)

        val metaQuery = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val (statQuery1, statQuery2) = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
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
        } else {
          s"""
             |DELETE FROM adsystemdb.tblADScurrency_rates
             |WHERE activity_date = ${value.activity_date}
             |AND to_currency = ${value.to_currency}
             |AND from_currency = ${value.from_currency}
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


    if (conf.getOrElse("debug", "false") == "true") tblADScurrency_rates.as[TblADScurrency_rates].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADScurrency_rates.as[TblADScurrency_rates].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADScurrency_ratesWriter).outputMode("append").start
    } else {
      tblADScurrency_rates.as[TblADScurrency_rates].writeStream.foreach(tblADScurrency_ratesWriter).outputMode("append").start
    }


    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


