package com.indeed.dataengineering.models

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._


case class TblCRMgeneric_product_credit(
                                         topic: String
                                         , partition: Int
                                         , offset: BigInt
                                         , opType: String
                                         , id: BigInt
                                         , activity_date: Date
                                         , advertiser_id: Option[BigInt]
                                         , relationship: String
                                         , user_id: Option[BigInt]
                                         , product_id: Option[BigInt]
                                         , revenue_generic_product_millicents: Option[BigDecimal]
                                         , revenue_generic_product_local: Option[BigInt]
                                         , currency: String
                                         , invoice_request_id: Option[BigInt]
                                         , rejected: Option[Int]
                                         , date_added: Timestamp
                                         , date_modified: Timestamp
                                       )


object TblCRMgeneric_product_credit {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("id", LongType)))),
    StructField("data", StructType(Array(
      StructField("id", LongType)
      , StructField("activity_date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("relationship", StringType)
      , StructField("user_id", LongType)
      , StructField("product_id", LongType)
      , StructField("revenue_generic_product_millicents", DecimalType(20, 0))
      , StructField("revenue_generic_product_local", LongType)
      , StructField("currency", StringType)
      , StructField("invoice_request_id", LongType)
      , StructField("rejected", IntegerType)
      , StructField("date_added", TimestampType)
      , StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("id", LongType)
      , StructField("activity_date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("relationship", StringType)
      , StructField("user_id", LongType)
      , StructField("product_id", LongType)
      , StructField("revenue_generic_product_millicents", DecimalType(20, 0))
      , StructField("revenue_generic_product_local", LongType)
      , StructField("currency", StringType)
      , StructField("invoice_request_id", LongType)
      , StructField("rejected", IntegerType)
      , StructField("date_added", TimestampType)
      , StructField("date_modified", TimestampType))))
  ))
}

