package com.indeed.dataengineering.models

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

case class TblADCsummary_advertiser(
                                     topic: String
                                     , partition: Int
                                     , offset: BigInt
                                     , opType: String
                                     , advertiser_id: Int
                                     , activity_date: Date
                                     , account_id: Option[Int]
                                     , masteraccount_id: Option[Int]
                                     , company: String
                                     , date_created: Date
                                     , active: Option[Int]
                                     , monthly_budget: Option[Double]
                                     , monthly_budget_local: Option[BigInt]
                                     , expended_budget: Option[Double]
                                     , expended_budget_local: Option[BigInt]
                                     , lifetime_budget_local: Option[BigInt]
                                     , expended_lifetime_budget_local: Option[BigInt]
                                     , `type`: String
                                     , payment_method: String
                                     , billing_threshold: Option[Double]
                                     , billing_threshold_local: Option[BigInt]
                                     , industry: String
                                     , is_ad_agency: Option[Int]
                                     , agency_discount: Option[Double]
                                     , process_level: String
                                     , estimated_budget: Option[Double]
                                     , estimated_budget_local: Option[BigInt]
                                     , first_revenue_date: Date
                                     , last_revenue_date: Date
                                     , account_email: String
                                     , billing_zip: String
                                     , billing_state: String
                                     , billing_country: String
                                     , entity: String
                                     , vat_country: String
                                     , vat_number: String
                                     , vat_exempt_13b_number: String
                                     , agency_rep: Option[Int]
                                     , sales_rep: Option[Int]
                                     , service_rep: Option[Int]
                                     , strategic_rep: Option[Int]
                                     , sj_count: Option[Int]
                                     , hj_count: Option[Int]
                                     , hjr_count: Option[Int]
                                     , daily_budget_hits_days: Option[Int]
                                     , daily_budget_hits_ads: Option[Int]
                                     , predicted_spend: Option[Double]
                                     , predicted_spend_local: Option[BigInt]
                                     , showHostedJobs: String
                                     , bid_managed: Option[Int]
                                     , companyPageStatus: String
                                     , sponJobIndeedApply: String
                                     , locale: String
                                     , last_note_date: Timestamp
                                     , last_email_date: Date
                                     , last_call_date: Date
                                     , last_meeting_date: Date
                                     , last_proposal_date: Date
                                     , last_activity_date: Date
                                     , parent_company_id: Option[Int]
                                     , currency: String
                                     , nps_likely: Option[Int]
                                     , nps_notusing_reason: String
                                     , commission_start_date: Date
                                     , commission_last_date: Date
                                     , employee_count: String
                                     , timestamp_created: Timestamp
                                     , date_modified: Timestamp
                                     , is_deleted: Boolean
                                     , binlog_timestamp: Timestamp
                                   )


object TblADCsummary_advertiser {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StructType(Array(
      StructField("advertiser_id", IntegerType)))),
    StructField("data", StructType(Array(
      StructField("advertiser_id", IntegerType)
      , StructField("account_id", IntegerType)
      , StructField("masteraccount_id", IntegerType)
      , StructField("company", StringType)
      , StructField("date_created", DateType)
      , StructField("active", IntegerType)
      , StructField("monthly_budget", DoubleType)
      , StructField("monthly_budget_local", LongType)
      , StructField("expended_budget", DoubleType)
      , StructField("expended_budget_local", LongType)
      , StructField("lifetime_budget_local", LongType)
      , StructField("expended_lifetime_budget_local", LongType)
      , StructField("type", StringType)
      , StructField("payment_method", StringType)
      , StructField("billing_threshold", DoubleType)
      , StructField("billing_threshold_local", LongType)
      , StructField("industry", StringType)
      , StructField("is_ad_agency", IntegerType)
      , StructField("agency_discount", DoubleType)
      , StructField("process_level", StringType)
      , StructField("estimated_budget", DoubleType)
      , StructField("estimated_budget_local", LongType)
      , StructField("first_revenue_date", DateType)
      , StructField("last_revenue_date", DateType)
      , StructField("account_email", StringType)
      , StructField("billing_zip", StringType)
      , StructField("billing_state", StringType)
      , StructField("billing_country", StringType)
      , StructField("entity", StringType)
      , StructField("vat_country", StringType)
      , StructField("vat_number", StringType)
      , StructField("vat_exempt_13b_number", StringType)
      , StructField("agency_rep", IntegerType)
      , StructField("sales_rep", IntegerType)
      , StructField("service_rep", IntegerType)
      , StructField("strategic_rep", IntegerType)
      , StructField("sj_count", IntegerType)
      , StructField("hj_count", IntegerType)
      , StructField("hjr_count", IntegerType)
      , StructField("daily_budget_hits_days", IntegerType)
      , StructField("daily_budget_hits_ads", IntegerType)
      , StructField("predicted_spend", DoubleType)
      , StructField("predicted_spend_local", LongType)
      , StructField("showHostedJobs", StringType)
      , StructField("bid_managed", IntegerType)
      , StructField("companyPageStatus", StringType)
      , StructField("sponJobIndeedApply", StringType)
      , StructField("locale", StringType)
      , StructField("last_note_date", TimestampType)
      , StructField("last_email_date", DateType)
      , StructField("last_call_date", DateType)
      , StructField("last_meeting_date", DateType)
      , StructField("last_proposal_date", DateType)
      , StructField("last_activity_date", DateType)
      , StructField("parent_company_id", IntegerType)
      , StructField("currency", StringType)
      , StructField("nps_likely", IntegerType)
      , StructField("nps_notusing_reason", StringType)
      , StructField("commission_start_date", DateType)
      , StructField("commission_last_date", DateType)
      , StructField("employee_count", StringType)
      , StructField("timestamp_created", TimestampType)
      , StructField("date_modified", TimestampType)
    ))),
    StructField("old", StructType(Array(
      StructField("advertiser_id", IntegerType)
      , StructField("account_id", IntegerType)
      , StructField("masteraccount_id", IntegerType)
      , StructField("company", StringType)
      , StructField("date_created", DateType)
      , StructField("active", IntegerType)
      , StructField("monthly_budget", DoubleType)
      , StructField("monthly_budget_local", LongType)
      , StructField("expended_budget", DoubleType)
      , StructField("expended_budget_local", LongType)
      , StructField("lifetime_budget_local", LongType)
      , StructField("expended_lifetime_budget_local", LongType)
      , StructField("type", StringType)
      , StructField("payment_method", StringType)
      , StructField("billing_threshold", DoubleType)
      , StructField("billing_threshold_local", LongType)
      , StructField("industry", StringType)
      , StructField("is_ad_agency", IntegerType)
      , StructField("agency_discount", DoubleType)
      , StructField("process_level", StringType)
      , StructField("estimated_budget", DoubleType)
      , StructField("estimated_budget_local", LongType)
      , StructField("first_revenue_date", DateType)
      , StructField("last_revenue_date", DateType)
      , StructField("account_email", StringType)
      , StructField("billing_zip", StringType)
      , StructField("billing_state", StringType)
      , StructField("billing_country", StringType)
      , StructField("entity", StringType)
      , StructField("vat_country", StringType)
      , StructField("vat_number", StringType)
      , StructField("vat_exempt_13b_number", StringType)
      , StructField("agency_rep", IntegerType)
      , StructField("sales_rep", IntegerType)
      , StructField("service_rep", IntegerType)
      , StructField("strategic_rep", IntegerType)
      , StructField("sj_count", IntegerType)
      , StructField("hj_count", IntegerType)
      , StructField("hjr_count", IntegerType)
      , StructField("daily_budget_hits_days", IntegerType)
      , StructField("daily_budget_hits_ads", IntegerType)
      , StructField("predicted_spend", DoubleType)
      , StructField("predicted_spend_local", LongType)
      , StructField("showHostedJobs", StringType)
      , StructField("bid_managed", IntegerType)
      , StructField("companyPageStatus", StringType)
      , StructField("sponJobIndeedApply", StringType)
      , StructField("locale", StringType)
      , StructField("last_note_date", TimestampType)
      , StructField("last_email_date", DateType)
      , StructField("last_call_date", DateType)
      , StructField("last_meeting_date", DateType)
      , StructField("last_proposal_date", DateType)
      , StructField("last_activity_date", DateType)
      , StructField("parent_company_id", IntegerType)
      , StructField("currency", StringType)
      , StructField("nps_likely", IntegerType)
      , StructField("nps_notusing_reason", StringType)
      , StructField("commission_start_date", DateType)
      , StructField("commission_last_date", DateType)
      , StructField("employee_count", StringType)
      , StructField("timestamp_created", TimestampType)
      , StructField("date_modified", TimestampType)
      )))
  ))
}

/*
Cassandra Create Table Stmt:

CREATE TABLE IF NOT EXISTS adcentraldb.tbladcsummary_advertiser
  (
     activity_date                  DATE,
     advertiser_id                  INT,
     account_id                     INT,
     masteraccount_id               INT,
     company                        TEXT,
     date_created                   DATE,
     active                         INT,
     monthly_budget                 DOUBLE,
     monthly_budget_local           BIGINT,
     expended_budget                DOUBLE,
     expended_budget_local          BIGINT,
     lifetime_budget_local          BIGINT,
     expended_lifetime_budget_local BIGINT,
     type                           TEXT,
     payment_method                 TEXT,
     billing_threshold              DOUBLE,
     billing_threshold_local        BIGINT,
     industry                       TEXT,
     is_ad_agency                   BOOLEAN,
     agency_discount                DOUBLE,
     process_level                  TEXT,
     estimated_budget               DOUBLE,
     estimated_budget_local         BIGINT,
     first_revenue_date             DATE,
     last_revenue_date              DATE,
     account_email                  TEXT,
     billing_zip                    TEXT,
     billing_state                  TEXT,
     billing_country                TEXT,
     entity                         TEXT,
     vat_country                    TEXT,
     vat_number                     TEXT,
     vat_exempt_13b_number          TEXT,
     agency_rep                     INT,
     sales_rep                      INT,
     service_rep                    INT,
     strategic_rep                  INT,
     sj_count                       INT,
     hj_count                       INT,
     hjr_count                      INT,
     daily_budget_hits_days         INT,
     daily_budget_hits_ads          INT,
     predicted_spend                DOUBLE,
     predicted_spend_local          BIGINT,
     showhostedjobs                 TEXT,
     bid_managed                    INT,
     companypagestatus              TEXT,
     sponjobindeedapply             TEXT,
     locale                         TEXT,
     last_note_date                 TIMESTAMP,
     last_email_date                DATE,
     last_call_date                 DATE,
     last_meeting_date              DATE,
     last_proposal_date             DATE,
     last_activity_date             DATE,
     parent_company_id              INT,
     currency                       TEXT,
     nps_likely                     INT,
     nps_notusing_reason            TEXT,
     commission_start_date          DATE,
     commission_last_date           DATE,
     employee_count                 TEXT,
     timestamp_created              TIMESTAMP,
     date_modified                  TIMESTAMP,
     is_deleted                     BOOLEAN,
     binlog_timestamp               TIMESTAMP,
     PRIMARY KEY (advertiser_id, activity_date)
  );
*/