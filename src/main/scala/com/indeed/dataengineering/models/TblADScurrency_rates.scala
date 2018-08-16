package com.indeed.dataengineering.models

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

case class TblADScurrency_rates(
                                 opType: String
                                 , activity_date: Date
                                 , from_currency: String
                                 , to_currency: String
                                 , exchange_rate: Option[BigInt]
                                 , buy_rate: Option[BigDecimal]
                                 , sell_rate: Option[BigDecimal]
                                 , source: String
                                 , date_modified: Timestamp
                               )


object TblADScurrency_rates {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("activity_date", DateType)
      , StructField("from_currency", StringType)
      , StructField("to_currency", StringType)))),
    StructField("data", StructType(Array(
      StructField("activity_date", DateType)
      , StructField("from_currency", StringType)
      , StructField("to_currency", StringType)
      , StructField("exchange_rate", LongType)
      , StructField("buy_rate", DecimalType(22, 12))
      , StructField("sell_rate", DecimalType(22, 12))
      , StructField("source", StringType)
      , StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("activity_date", DateType)
      , StructField("from_currency", StringType)
      , StructField("to_currency", StringType)
      , StructField("exchange_rate", LongType)
      , StructField("buy_rate", DecimalType(22, 12))
      , StructField("sell_rate", DecimalType(22, 12))
      , StructField("source", StringType)
      , StructField("date_modified", TimestampType))))
  ))
}
