package com.indeed.dataengineering.models

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

case class TblADCadvertiser_rep_revenues(
                                          opType: String
                                          , activity_date: Date
                                          , advertiser_id: BigInt
                                          , relationship: String
                                          , user_id: Option[BigInt]
                                          , revenue: Option[BigDecimal]
                                          , revenue_jobsearch_millicents: Option[BigInt]
                                          , revenue_resume_millicents: Option[BigInt]
                                          , revenue_jobsearch_local: Option[BigInt]
                                          , revenue_resume_local: Option[BigInt]
                                          , currency: String
                                          , revenue_ss_millicents: Option[BigDecimal]
                                          , revenue_hj_millicents: Option[BigDecimal]
                                          , revenue_hjr_millicents: Option[BigDecimal]
                                          , revenue_ss_local: Option[BigInt]
                                          , revenue_hj_local: Option[BigInt]
                                          , revenue_hjr_local: Option[BigInt]
                                          , date_modified: Timestamp
                                        )


object TblADCadvertiser_rep_revenues {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("activity_date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("relationship", StringType)))),
    StructField("data", StructType(Array(
      StructField("activity_date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("relationship", StringType)
      , StructField("user_id", LongType)
      , StructField("revenue", DecimalType(7, 2))
      , StructField("revenue_jobsearch_millicents", LongType)
      , StructField("revenue_resume_millicents", LongType)
      , StructField("revenue_jobsearch_local", LongType)
      , StructField("revenue_resume_local", LongType)
      , StructField("currency", StringType)
      , StructField("revenue_ss_millicents", DecimalType(20, 0))
      , StructField("revenue_hj_millicents", DecimalType(20, 0))
      , StructField("revenue_hjr_millicents", DecimalType(20, 0))
      , StructField("revenue_ss_local", LongType)
      , StructField("revenue_hj_local", LongType)
      , StructField("revenue_hjr_local", LongType)
      , StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("activity_date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("relationship", StringType)
      , StructField("user_id", LongType)
      , StructField("revenue", DecimalType(7, 2))
      , StructField("revenue_jobsearch_millicents", LongType)
      , StructField("revenue_resume_millicents", LongType)
      , StructField("revenue_jobsearch_local", LongType)
      , StructField("revenue_resume_local", LongType)
      , StructField("currency", StringType)
      , StructField("revenue_ss_millicents", DecimalType(20, 0))
      , StructField("revenue_hj_millicents", DecimalType(20, 0))
      , StructField("revenue_hjr_millicents", DecimalType(20, 0))
      , StructField("revenue_ss_local", LongType)
      , StructField("revenue_hj_local", LongType)
      , StructField("revenue_hjr_local", LongType)
      , StructField("date_modified", TimestampType))))
  ))
}


