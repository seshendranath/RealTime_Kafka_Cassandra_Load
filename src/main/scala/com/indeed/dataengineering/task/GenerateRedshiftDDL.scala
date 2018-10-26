package com.indeed.dataengineering.task

import com.indeed.dataengineering.GenericDaemon.conf
import com.indeed.dataengineering.utilities.{Logging, SqlJDBC}
import com.indeed.dataengineering.utilities.Utils._

class GenerateRedshiftDDL extends Logging {
  def run(): Unit = {
    log.info("Creating Postgresql connection")
    val postgresql = new SqlJDBC("postgresql", conf("metadata.url"), conf("metadata.user"), conf("metadata.password"))

    val redshift = new SqlJDBC("redshift", conf("redshift.url"), conf("redshift.user"), conf("redshift.password"))

    val pattern = "\\((\\w+),(\\w+)\\)".r
    conf("tables").split(";").foreach(println)
    val keyMap = conf("tables").split(";").map{t => val pattern(tbl, key) = t; (tbl, key)}.toMap

    val tables = keyMap.keys.toSet

    log.info(s"Building metadata for whitelisted tables $tables")
    val res = buildMetadata(postgresql, tables)

    val redshiftKeywords = Set("partition", "offset", "type", "year", "month")

    tables.foreach { tbl =>
      val dropQuery = s"DROP TABLE IF EXISTS ${conf("redshift.schema")}.$tbl;"
      val createQuery = s"""CREATE TABLE IF NOT EXISTS ${conf("redshift.schema")}.$tbl\n(\ntopic VARCHAR(256)\n,"partition" INTEGER\n,"offset" BIGINT\n,optype VARCHAR(30)\n,""" + res(tbl).map(c => (if (redshiftKeywords contains c._1) s""""${c._1}"""" else c._1)+ " " + postgresqlToRedshiftDataType(c._2)).mkString("\n,") + s"\n) DISTSTYLE KEY DISTKEY (${keyMap(tbl)});"

      redshift.executeUpdate(dropQuery)
      redshift.executeUpdate(createQuery)
    }
  }

}
