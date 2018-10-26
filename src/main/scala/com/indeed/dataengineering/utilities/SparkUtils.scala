package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */


import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.indeed.dataengineering.utilities.Utils._
import scala.collection.mutable


object SparkUtils extends Logging {

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


  def postgresqlToSparkDataType(dataType: String): DataType = {

    val varcharPattern = """VARCHAR\(\d+\)""".r

    dataType match {
      case "DATETIME" => TimestampType
      case "TIME" => StringType
      case "DATE" => DateType
      case "SMALLINT" => IntegerType
      case "TIMESTAMP" => TimestampType
      case "FLOAT" => DoubleType
      case "INTEGER" => IntegerType
      case varcharPattern() => StringType
      case "NUMERIC" => DoubleType
      case "BIGINT" => LongType
      case "UUID" => StringType
      case "BOOLEAN" => IntegerType
      case "DOUBLE" => DoubleType
      case "YEAR" => IntegerType
      case _ => StringType
    }
  }

}
