package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */

import com.github.nscala_time.time.Imports.DateTime
import scala.collection.mutable
import org.apache.spark.sql._

object Utils {

  val quoteTypes = Set("StringType", "DateType", "TimestampType")

  def sparkToScalaDataType(dataType: String): String = {
    val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

    dataType match {
      case "StringType" => "String"
      case "LongType" => "BigInt"
      case "DoubleType" => "Double"
      case "IntegerType" => "Int"
      case "DateType" => "Date"
      case "TimestampType" => "Timestamp"
      case "BooleanType" => "Boolean"
      case decimalPattern() => "BigDecimal" //"Double"// dataType.replace("Type", "").toLowerCase
      case _ => "String"
    }
  }


  def getScalaType(pk: Set[String], col: String, dtype: String): String = {
    if ((quoteTypes contains dtype) || (pk contains col)) sparkToScalaDataType(dtype) else "Option[" + sparkToScalaDataType(dtype) + "]"
  }


  def sparkToCassandraDataType(dataType: String): String = {
    val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

    dataType match {
      case "StringType" => "text"
      case "LongType" => "bigint"
      case "DoubleType" => "double"
      case "IntegerType" => "int"
      case "DateType" => "date"
      case "TimestampType" => "timestamp"
      case "BooleanType" => "boolean"
      case decimalPattern() => "decimal" // "double"// dataType.replace("Type", "").toLowerCase
      case _ => "text"
    }
  }


  def getColsFromDF(df: DataFrame, exclude: Seq[String] = Seq()): Array[(String, String)] = {
    val cols = mutable.ArrayBuffer[(String, String)]()
    for (column <- df.dtypes) {
      val (col, dataType) = column
      if (!(exclude contains col)) {
        cols += ((col, sparkToCassandraDataType(dataType)))
      }
    }
    cols.toArray
  }


  def getSetClause(opType: String): String = {
    if (opType.startsWith("i")) s"${opType}ed_records = ${opType}ed_records + 1" else s"${opType}d_records = ${opType}d_records + 1"
  }


  def getMetaQueries(className: String, db: String, tbl: String, topic: String, partition: Int, offset: BigInt): String = {
    val metaQuery =
      s"""
         |INSERT INTO metadata.streaming_metadata (job, db, tbl, topic, partition, offset)
         |VALUES ('$className', '$db', '$tbl', '$topic', $partition, $offset)
    	 """.stripMargin

    metaQuery
  }


  def getStatQueries(setClause: String, className: String, db: String, tbl: String): (String, String) = {

    val statQuery1 =
      s"""
         |UPDATE stats.streaming_stats
         |SET $setClause, total_records_processed = total_records_processed + 1
         |WHERE job = '$className' AND db = '$db' and tbl = '$tbl'
     """.stripMargin

    val statQuery2 =
      s"""
         |UPDATE stats.streaming_stats_by_hour
         |SET $setClause, total_records_processed = total_records_processed + 1
         |WHERE job = '$className' AND db = '$db' and tbl = '$tbl' AND dt = '${DateTime.now.toString("yyyy-MM-dd")}' AND hr = ${DateTime.now.getHourOfDay}
       """.stripMargin

    (statQuery1, statQuery2)
  }

}
