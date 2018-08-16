package com.indeed.dataengineering.models


import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

case class Tbladvertiser(
                          opType: String
                          , id: BigInt
                          , account_id: Option[BigInt]
                          , company: String
                          , contact: String
                          , url: String
                          , address1: String
                          , address2: String
                          , city: String
                          , state: String
                          , zip: String
                          , phone: String
                          , phone_type: String
                          , verified_phone: String
                          , verified_phone_extension: String
                          , uuidstring: String
                          , date_created: Timestamp
                          , active: Option[Int]
                          , ip_address: String
                          , referral_id: Option[BigInt]
                          , monthly_budget: Option[BigDecimal]
                          , expended_budget: Option[BigDecimal]
                          , `type`: String
                          , advertiser_number: String
                          , show_conversions: Option[Int]
                          , billing_threshold: Option[BigDecimal]
                          , payment_method: String
                          , industry: String
                          , agency_discount: Option[BigDecimal]
                          , is_ad_agency: Option[Boolean]
                          , process_level: String
                          , estimated_budget: Option[BigDecimal]
                          , first_revenue_date: Date
                          , last_revenue_date: Date
                          , first_revenue_override: Date
                          , terms: String
                          , currency: String
                          , monthly_budget_local: Option[BigInt]
                          , expended_budget_local: Option[BigInt]
                          , billing_threshold_local: Option[BigDecimal]
                          , estimated_budget_local: Option[BigInt]
                          , employee_count: String
                          , last_updated: Timestamp
                        )


object Tbladvertiser {
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
      , StructField("account_id", LongType)
      , StructField("company", StringType)
      , StructField("contact", StringType)
      , StructField("url", StringType)
      , StructField("address1", StringType)
      , StructField("address2", StringType)
      , StructField("city", StringType)
      , StructField("state", StringType)
      , StructField("zip", StringType)
      , StructField("phone", StringType)
      , StructField("phone_type", StringType)
      , StructField("verified_phone", StringType)
      , StructField("verified_phone_extension", StringType)
      , StructField("uuidstring", StringType)
      , StructField("date_created", TimestampType)
      , StructField("active", IntegerType)
      , StructField("ip_address", StringType)
      , StructField("referral_id", LongType)
      , StructField("monthly_budget", DecimalType(10, 2))
      , StructField("expended_budget", DecimalType(10, 2))
      , StructField("type", StringType)
      , StructField("advertiser_number", StringType)
      , StructField("show_conversions", IntegerType)
      , StructField("billing_threshold", DecimalType(10, 2))
      , StructField("payment_method", StringType)
      , StructField("industry", StringType)
      , StructField("agency_discount", DecimalType(10, 3))
      , StructField("is_ad_agency", BooleanType)
      , StructField("process_level", StringType)
      , StructField("estimated_budget", DecimalType(7, 2))
      , StructField("first_revenue_date", DateType)
      , StructField("last_revenue_date", DateType)
      , StructField("first_revenue_override", DateType)
      , StructField("terms", StringType)
      , StructField("currency", StringType)
      , StructField("monthly_budget_local", LongType)
      , StructField("expended_budget_local", LongType)
      , StructField("billing_threshold_local", DecimalType(20, 0))
      , StructField("estimated_budget_local", LongType)
      , StructField("employee_count", StringType)
      , StructField("last_updated", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("id", LongType)
      , StructField("account_id", LongType)
      , StructField("company", StringType)
      , StructField("contact", StringType)
      , StructField("url", StringType)
      , StructField("address1", StringType)
      , StructField("address2", StringType)
      , StructField("city", StringType)
      , StructField("state", StringType)
      , StructField("zip", StringType)
      , StructField("phone", StringType)
      , StructField("phone_type", StringType)
      , StructField("verified_phone", StringType)
      , StructField("verified_phone_extension", StringType)
      , StructField("uuidstring", StringType)
      , StructField("date_created", TimestampType)
      , StructField("active", IntegerType)
      , StructField("ip_address", StringType)
      , StructField("referral_id", LongType)
      , StructField("monthly_budget", DecimalType(10, 2))
      , StructField("expended_budget", DecimalType(10, 2))
      , StructField("type", StringType)
      , StructField("advertiser_number", StringType)
      , StructField("show_conversions", IntegerType)
      , StructField("billing_threshold", DecimalType(10, 2))
      , StructField("payment_method", StringType)
      , StructField("industry", StringType)
      , StructField("agency_discount", DecimalType(10, 3))
      , StructField("is_ad_agency", BooleanType)
      , StructField("process_level", StringType)
      , StructField("estimated_budget", DecimalType(7, 2))
      , StructField("first_revenue_date", DateType)
      , StructField("last_revenue_date", DateType)
      , StructField("first_revenue_override", DateType)
      , StructField("terms", StringType)
      , StructField("currency", StringType)
      , StructField("monthly_budget_local", LongType)
      , StructField("expended_budget_local", LongType)
      , StructField("billing_threshold_local", DecimalType(20, 0))
      , StructField("estimated_budget_local", LongType)
      , StructField("employee_count", StringType)
      , StructField("last_updated", TimestampType))))
  ))
}
