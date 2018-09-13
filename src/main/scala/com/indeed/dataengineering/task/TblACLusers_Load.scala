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


class TblACLusers_Load {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {

    import spark.implicits._

    val db = "adcentraldb"
    val tbl = "tblACLusers"

    val className = this.getClass.getSimpleName

    val checkpointDir = conf("checkpointBaseLoc") + className

    val executePlain = conf.getOrElse("executePlain", "false").toBoolean
    val executeMeta = conf.getOrElse("executeMeta", "false").toBoolean

    log.info("Map extracted kafka consumer records to Case Class")
    val tblACLusers = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblACLusers.jsonSchema).as("value")).filter($"value.table" === "tblACLusers").select($"topic", $"partition", $"offset", $"value.type".as("opType"), $"value.data.*").where("opType IN ('insert', 'update', 'delete')")


    log.info("Create ForeachWriter for Cassandra")
    val tblACLusersWriter = new ForeachWriter[TblACLusers] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: TblACLusers): Unit = {

        val setClause = getSetClause(value.opType)

        val metaQuery = getMetaQueries(className, db, tbl, value.topic, value.partition, value.offset)

        val (statQuery1, statQuery2) = getStatQueries(setClause, className, db, tbl)

        val cQuery1 = if (value.opType == "insert" || value.opType == "update") {
            s"""
               |INSERT INTO adcentraldb.tblACLusers (id,firstname,lastname,email,username,active,createdate,timezone,entity,adp_file_number,intacct_department_id,workday_id,workday_team,date_created,date_modified)
               |VALUES (
               | ${value.id}
               |,${if (value.firstname == null) null else "'" + value.firstname.replaceAll("'", "''") + "'"}
               |,${if (value.lastname == null) null else "'" + value.lastname.replaceAll("'", "''") + "'"}
               |,${if (value.email == null) null else "'" + value.email.replaceAll("'", "''") + "'"}
               |,${if (value.username == null) null else "'" + value.username.replaceAll("'", "''") + "'"}
               |,${value.active.orNull}
               |,${if (value.createdate == null) null else "'" + value.createdate + "'"}
               |,${value.timezone.orNull}
               |,${if (value.entity == null) null else "'" + value.entity.replaceAll("'", "''") + "'"}
               |,${value.adp_file_number.orNull}
               |,${if (value.intacct_department_id == null) null else "'" + value.intacct_department_id.replaceAll("'", "''") + "'"}
               |,${if (value.workday_id == null) null else "'" + value.workday_id.replaceAll("'", "''") + "'"}
               |,${if (value.workday_team == null) null else "'" + value.workday_team.replaceAll("'", "''") + "'"}
               |,${if (value.date_created == null) null else "'" + value.date_created + "'"}
               |,${if (value.date_modified == null) null else "'" + value.date_modified + "'"}
               |)
             """.stripMargin
        } else {
            s"""
               |DELETE FROM adcentraldb.tblACLusers
               |WHERE id = ${value.id}
             """.stripMargin
        }

        connector.withSessionDo { session =>

          if (executePlain) {
            session.execute(cQuery1)
          } else if (executeMeta) {
            /* val batchStatement1 = new BatchStatement
            batchStatement1.add(session.prepare(cQuery1).bind)
            batchStatement1.add(session.prepare(metaQuery).bind)
            session.execute(batchStatement1) */

            session.execute(cQuery1)
            session.execute(metaQuery)
          } else {
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


    if (conf.getOrElse("debug", "false") == "true") tblACLusers.as[TblACLusers].writeStream.format("console").outputMode(conf.getOrElse("outputMode", "update")).start()

    // log.info("Cleanup Checkpoint Dir")
    // if (dfs.exists(new Path(checkpointDir))) dfc.delete(new Path(checkpointDir), true)

    log.info("Write Streams to Cassandra Table")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      tblACLusers.as[TblACLusers].writeStream.option("checkpointLocation", checkpointDir).foreach(tblACLusersWriter).outputMode("append").start
    } else {
      tblACLusers.as[TblACLusers].writeStream.foreach(tblACLusersWriter).outputMode("append").start
    }

    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination

  }

}


