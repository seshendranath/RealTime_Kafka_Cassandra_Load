package com.indeed.dataengineering.task

/**
  * Created by aguyyala on 2/16/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.utilities.Utils._

class AutoGenerateScalaCode {

  def run(): Unit = {

    val db = conf("db")
    val table = conf("table")
    val pk = conf("pk").split(",").toSet

    val df: DataFrame = spark.read.parquet(conf.getOrElse("sourcePath", conf("s3aUri") + conf("s3Bucket") + "/" + conf("basePath") + "/" + s"$db/$table"))
    val cols: String = df.columns.mkString(",")

    // Produce Case Statement
    val caseStmt: String = "case class " + table.capitalize + "(\n opType: String\n" + df.dtypes.map { case (col, dtype) => "," + col + ": " + getScalaType(pk, col, dtype) }.mkString("\n") + "\n)"

    // Generate PK Statement
    val pkSchema: String = "\n\t\t\t\t\t\t " + df.dtypes.filter { case (col, _) => pk contains col }.map { case (col, dtype) => s"""StructField("$col", ${convertBoolToInt(dtype)})""" }.mkString("\n\t\t\t\t\t\t,")

    // Generate Data Schema
    val tableSchema: String = "\n\t\t\t\t\t\t " + df.dtypes.map { case (col, dtype) => s"""StructField("$col", ${convertBoolToInt(dtype)})""" }.mkString("\n\t\t\t\t\t\t,")

    // Generates JSON Schema
    val jSchema: String =
      s"""
         |object ${table.capitalize} {
         |  val jsonSchema = StructType(Array(
         |          StructField("database", StringType),
         |          StructField("table", StringType),
         |          StructField("type", StringType),
         |          StructField("ts", TimestampType),
         |          StructField("position", StringType),
         |          StructField("primary_key", StructType(Array($pkSchema))),
         |          StructField("data", StructType(Array($tableSchema))),
         |          StructField("old", StructType(Array($tableSchema)))
         |          ))
         |}
       """.stripMargin

    // Generates val stmt
    val valStmt =
      s"""val $table = rawData.select(from_json($$"value", ${table.capitalize}.jsonSchema).as("value")).filter($$"value.table" === "$table").select($$"value.type".as("opType"), $$"value.data.*")"""

    // Generates insert values stmt
    val insertValues = s"|INSERT INTO $db.$table ($cols)\n|VALUES (" + "\n| " + df.dtypes.map { case (col, dtype) => if (pk contains col) {
      if (quoteTypes contains dtype) s"""'$${value.$col}'""" else s"""$${value.$col}"""
    } else if (dtype == "StringType") s"""$${if (value.$col == null) null else "'" + value.$col.replaceAll("'", "''")+ "'"}""" else if (quoteTypes contains dtype) s"""$${if (value.$col == null) null else "'" + value.$col+ "'"}""" else s"""$${value.$col.orNull}"""
    }.mkString("\n|,") + "\n|)"

    val deleteValues: String = s"|DELETE FROM $db.$table\n|WHERE " + pk.map(c => s"$c = $${value.$c}").mkString("\n|AND ")

    // Generates ForeachWriter
    val writer: String =
      s"""
         |val ${table}Writer = new ForeachWriter[${table.capitalize}] {
         |
         |  def open(partitionId: Long, version: Long): Boolean = true
         |
         |  def process(value: ${table.capitalize}): Unit = {
         |
         |    def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true
         |
         |    if (value.opType == "insert" || value.opType == "update")
         |      {
         |        val cQuery1 =
         |                s${'"'}${'"'}${'"'}
         |                $insertValues
         |                ${'"'}${'"'}${'"'}.stripMargin
         |        connector.withSessionDo{session =>session.execute(cQuery1)}
         |      }
         |    else if (value.opType == "delete")
         |      {
         |        val cQuery1 =
         |                s${'"'}${'"'}${'"'}
         |                $deleteValues
         |                ${'"'}${'"'}${'"'}.stripMargin
         |        connector.withSessionDo{session =>session.execute(cQuery1)}
         |      }
         |  }
         |  def close(errorOrNull: Throwable): Unit = {}
         |}
       """.stripMargin


    val streamStmt = s"""val ${table}Query = $table.as[${table.capitalize}].writeStream.foreach(${table}Writer).outputMode("append").start"""

    log.info(caseStmt)
    log.info(jSchema)
    log.info(valStmt)
    log.info(writer)
    log.info(streamStmt)

  }

}
