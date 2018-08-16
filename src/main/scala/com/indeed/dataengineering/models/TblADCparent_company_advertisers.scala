package com.indeed.dataengineering.models

import java.sql.Timestamp
import org.apache.spark.sql.types._

case class TblADCparent_company_advertisers(
                                             opType: String
                                             , parent_company_id: BigInt
                                             , advertiser_id: BigInt
                                             , date_created: Timestamp
                                             , assignment_method: String
                                             , assigned_by: Option[BigInt]
                                             , date_modified: Timestamp
                                           )


object TblADCparent_company_advertisers {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("parent_company_id", LongType)
      , StructField("advertiser_id", LongType)))),
    StructField("data", StructType(Array(
      StructField("parent_company_id", LongType)
      , StructField("advertiser_id", LongType)
      , StructField("date_created", TimestampType)
      , StructField("assignment_method", StringType)
      , StructField("assigned_by", LongType)
      , StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("parent_company_id", LongType)
      , StructField("advertiser_id", LongType)
      , StructField("date_created", TimestampType)
      , StructField("assignment_method", StringType)
      , StructField("assigned_by", LongType)
      , StructField("date_modified", TimestampType))))
  ))
}
