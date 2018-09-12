package com.indeed.dataengineering.models

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

case class TblADCaccounts_salesrep_commissions(
                                                topic: String
                                                , partition: Int
                                                , offset: BigInt
                                                , opType: String
                                                , date: Date
                                                , advertiser_id: BigInt
                                                , salesrep_id: Option[BigInt]
                                                , revenue_jobsearch_millicents: Option[BigInt]
                                                , revenue_dradis_lifetime_millicents: Option[BigDecimal]
                                                , revenue_dradis_recurring_millicents: Option[BigDecimal]
                                                , revenue_resume_millicents: Option[BigDecimal]
                                                , revenue_ineligible_millicents: Option[BigDecimal]
                                                , revenue_jobsearch_local: Option[BigInt]
                                                , revenue_dradis_lifetime_local: Option[BigInt]
                                                , revenue_dradis_recurring_local: Option[BigInt]
                                                , revenue_resume_local: Option[BigInt]
                                                , revenue_ineligible_local: Option[BigInt]
                                                , discount_local: Option[BigInt]
                                                , discount_forward_local: Option[BigInt]
                                                , discounted_revenue_local: Option[BigInt]
                                                , commission_rate: Option[BigDecimal]
                                                , commission_amount_local: Option[BigInt]
                                                , commission_amount_millicents: Option[BigDecimal]
                                                , newrevenue_jobsearch_millicents: Option[BigDecimal]
                                                , newrevenue_dradis_lifetime_millicents: Option[BigDecimal]
                                                , newrevenue_dradis_recurring_millicents: Option[BigDecimal]
                                                , newrevenue_resume_millicents: Option[BigDecimal]
                                                , currency: String
                                                , date_modified: Timestamp
                                              )


object TblADCaccounts_salesrep_commissions {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("date", DateType)
      , StructField("advertiser_id", LongType)))),
    StructField("data", StructType(Array(
      StructField("date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("salesrep_id", LongType)
      , StructField("revenue_jobsearch_millicents", LongType)
      , StructField("revenue_dradis_lifetime_millicents", DecimalType(20, 0))
      , StructField("revenue_dradis_recurring_millicents", DecimalType(20, 0))
      , StructField("revenue_resume_millicents", DecimalType(20, 0))
      , StructField("revenue_ineligible_millicents", DecimalType(20, 0))
      , StructField("revenue_jobsearch_local", LongType)
      , StructField("revenue_dradis_lifetime_local", LongType)
      , StructField("revenue_dradis_recurring_local", LongType)
      , StructField("revenue_resume_local", LongType)
      , StructField("revenue_ineligible_local", LongType)
      , StructField("discount_local", LongType)
      , StructField("discount_forward_local", LongType)
      , StructField("discounted_revenue_local", LongType)
      , StructField("commission_rate", DecimalType(3, 3))
      , StructField("commission_amount_local", LongType)
      , StructField("commission_amount_millicents", DecimalType(20, 0))
      , StructField("newrevenue_jobsearch_millicents", DecimalType(20, 0))
      , StructField("newrevenue_dradis_lifetime_millicents", DecimalType(20, 0))
      , StructField("newrevenue_dradis_recurring_millicents", DecimalType(20, 0))
      , StructField("newrevenue_resume_millicents", DecimalType(20, 0))
      , StructField("currency", StringType)
      , StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("date", DateType)
      , StructField("advertiser_id", LongType)
      , StructField("salesrep_id", LongType)
      , StructField("revenue_jobsearch_millicents", LongType)
      , StructField("revenue_dradis_lifetime_millicents", DecimalType(20, 0))
      , StructField("revenue_dradis_recurring_millicents", DecimalType(20, 0))
      , StructField("revenue_resume_millicents", DecimalType(20, 0))
      , StructField("revenue_ineligible_millicents", DecimalType(20, 0))
      , StructField("revenue_jobsearch_local", LongType)
      , StructField("revenue_dradis_lifetime_local", LongType)
      , StructField("revenue_dradis_recurring_local", LongType)
      , StructField("revenue_resume_local", LongType)
      , StructField("revenue_ineligible_local", LongType)
      , StructField("discount_local", LongType)
      , StructField("discount_forward_local", LongType)
      , StructField("discounted_revenue_local", LongType)
      , StructField("commission_rate", DecimalType(3, 3))
      , StructField("commission_amount_local", LongType)
      , StructField("commission_amount_millicents", DecimalType(20, 0))
      , StructField("newrevenue_jobsearch_millicents", DecimalType(20, 0))
      , StructField("newrevenue_dradis_lifetime_millicents", DecimalType(20, 0))
      , StructField("newrevenue_dradis_recurring_millicents", DecimalType(20, 0))
      , StructField("newrevenue_resume_millicents", DecimalType(20, 0))
      , StructField("currency", StringType)
      , StructField("date_modified", TimestampType))))
  ))
}