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


class TblADCquota_Load {

  def run(rawData: DataFrame, connector: CassandraConnector): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblADCquota"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tblADCquota = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCquota.jsonSchema).as("value")).filter($"value.table" === "tblADCquota").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tblADCquotaWriter = new ForeachWriter[TblADCquota] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblADCquota): Unit = {

        val setClause = getSetClause(value.opType)

        val metaQuery = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val (statQuery1, statQuery2) = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
          s"""
             |INSERT INTO adcentraldb.tblADCquota (year,month,user_id,quota_type,quota,quota_local,currency,date_added,date_modified,edited_by)
             |VALUES (
             | ${value.year}
             |,${value.month}
             |,${value.user_id}
             |,'${value.quota_type}'
             |,${value.quota.orNull}
             |,${value.quota_local.orNull}
             |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''") + "'"}
             |,${if (value.date_added == null) null else "'" + value.date_added + "'"}
             |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
             |,${value.edited_by.orNull}
             |)
           """.stripMargin
        } else {
          s"""
             |DELETE FROM adcentraldb.tblADCquota
             |WHERE year = ${value.year}
             |AND month = ${value.month}
             |AND user_id = ${value.user_id}
             |AND quota_type = ${value.quota_type}
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


    if (conf.getOrElse("debug", "false") == "true") tblADCquota.as[TblADCquota].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblADCquota.as[TblADCquota].writeStream.option("checkpointLocation", checkpointDir).foreach(tblADCquotaWriter).outputMode("append").start
    } else {
      tblADCquota.as[TblADCquota].writeStream.foreach(tblADCquotaWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


