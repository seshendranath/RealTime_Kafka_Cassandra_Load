package com.indeed.dataengineering.models

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

case class TblADCparent_company(
                                 topic: String
                                 , partition: Int
                                 , offset: BigInt
                                 , opType: String
                                 , id: BigInt
                                 , company: String
                                 , sales_region: String
                                 , user_id: Option[BigInt]
                                 , first_revenue_date: Date
                                 , default_advertiser_id: Option[BigInt]
                                 , prospecting_status: String
                                 , hq_city: String
                                 , hq_state: String
                                 , hq_zip: String
                                 , hq_country: String
                                 , duns: String
                                 , location_type: String
                                 , revenue: Option[BigInt]
                                 , total_employees: Option[Int]
                                 , franchise_operation_type: String
                                 , is_subsidiary: Option[Int]
                                 , doing_business_as: String
                                 , exchange_symbol: String
                                 , exchange: String
                                 , USSIC: String
                                 , USSIC_description: String
                                 , NAICS: String
                                 , NAICS_description: String
                                 , parent_name: String
                                 , parent_duns: String
                                 , ultimate_domestic_parent_name: String
                                 , ultimate_domestic_parent_duns: String
                                 , ultimate_parent_name: String
                                 , ultimate_parent_duns: String
                                 , active_jobs: Option[BigInt]
                                 , date_created: Timestamp
                                 , is_lead_eligible: Option[Int]
                                 , lead_score: Option[Int]
                                 , date_modified: Timestamp
                               )


object TblADCparent_company {
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
      , StructField("company", StringType)
      , StructField("sales_region", StringType)
      , StructField("user_id", LongType)
      , StructField("first_revenue_date", DateType)
      , StructField("default_advertiser_id", LongType)
      , StructField("prospecting_status", StringType)
      , StructField("hq_city", StringType)
      , StructField("hq_state", StringType)
      , StructField("hq_zip", StringType)
      , StructField("hq_country", StringType)
      , StructField("duns", StringType)
      , StructField("location_type", StringType)
      , StructField("revenue", LongType)
      , StructField("total_employees", IntegerType)
      , StructField("franchise_operation_type", StringType)
      , StructField("is_subsidiary", IntegerType)
      , StructField("doing_business_as", StringType)
      , StructField("exchange_symbol", StringType)
      , StructField("exchange", StringType)
      , StructField("USSIC", StringType)
      , StructField("USSIC_description", StringType)
      , StructField("NAICS", StringType)
      , StructField("NAICS_description", StringType)
      , StructField("parent_name", StringType)
      , StructField("parent_duns", StringType)
      , StructField("ultimate_domestic_parent_name", StringType)
      , StructField("ultimate_domestic_parent_duns", StringType)
      , StructField("ultimate_parent_name", StringType)
      , StructField("ultimate_parent_duns", StringType)
      , StructField("active_jobs", LongType)
      , StructField("date_created", TimestampType)
      , StructField("is_lead_eligible", IntegerType)
      , StructField("lead_score", IntegerType)
      , StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("id", LongType)
      , StructField("company", StringType)
      , StructField("sales_region", StringType)
      , StructField("user_id", LongType)
      , StructField("first_revenue_date", DateType)
      , StructField("default_advertiser_id", LongType)
      , StructField("prospecting_status", StringType)
      , StructField("hq_city", StringType)
      , StructField("hq_state", StringType)
      , StructField("hq_zip", StringType)
      , StructField("hq_country", StringType)
      , StructField("duns", StringType)
      , StructField("location_type", StringType)
      , StructField("revenue", LongType)
      , StructField("total_employees", IntegerType)
      , StructField("franchise_operation_type", StringType)
      , StructField("is_subsidiary", IntegerType)
      , StructField("doing_business_as", StringType)
      , StructField("exchange_symbol", StringType)
      , StructField("exchange", StringType)
      , StructField("USSIC", StringType)
      , StructField("USSIC_description", StringType)
      , StructField("NAICS", StringType)
      , StructField("NAICS_description", StringType)
      , StructField("parent_name", StringType)
      , StructField("parent_duns", StringType)
      , StructField("ultimate_domestic_parent_name", StringType)
      , StructField("ultimate_domestic_parent_duns", StringType)
      , StructField("ultimate_parent_name", StringType)
      , StructField("ultimate_parent_duns", StringType)
      , StructField("active_jobs", LongType)
      , StructField("date_created", TimestampType)
      , StructField("is_lead_eligible", IntegerType)
      , StructField("lead_score", IntegerType)
      , StructField("date_modified", TimestampType))))
  ))
}
