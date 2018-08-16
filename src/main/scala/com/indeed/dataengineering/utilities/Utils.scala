package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */


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

}
