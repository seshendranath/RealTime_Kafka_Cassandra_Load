package com.indeed.dataengineering.models

import java.sql.Timestamp
import org.apache.spark.sql.types._

case class TblADCquota(
                        opType: String
                        , year: BigInt
                        , month: Int
                        , user_id: Int
                        , quota_type: String
                        , quota: Option[BigDecimal]
                        , quota_local: Option[BigInt]
                        , currency: String
                        , date_added: Timestamp
                        , date_modified: Timestamp
                        , edited_by: Option[Int]
                      )


object TblADCquota {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("year", LongType)
      , StructField("month", IntegerType)
      , StructField("user_id", IntegerType)
      , StructField("quota_type", StringType)))),
    StructField("data", StructType(Array(
      StructField("year", LongType)
      , StructField("month", IntegerType)
      , StructField("user_id", IntegerType)
      , StructField("quota_type", StringType)
      , StructField("quota", DecimalType(9, 2))
      , StructField("quota_local", LongType)
      , StructField("currency", StringType)
      , StructField("date_added", TimestampType)
      , StructField("date_modified", TimestampType)
      , StructField("edited_by", IntegerType)))),
    StructField("old", StructType(Array(
      StructField("year", LongType)
      , StructField("month", IntegerType)
      , StructField("user_id", IntegerType)
      , StructField("quota_type", StringType)
      , StructField("quota", DecimalType(9, 2))
      , StructField("quota_local", LongType)
      , StructField("currency", StringType)
      , StructField("date_added", TimestampType)
      , StructField("date_modified", TimestampType)
      , StructField("edited_by", IntegerType))))
  ))
}

