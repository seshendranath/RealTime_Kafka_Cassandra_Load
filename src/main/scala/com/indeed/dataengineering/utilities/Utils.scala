package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */

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

}
