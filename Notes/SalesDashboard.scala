// Testing Real Time Sales Dashboard in Spark Shell
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core._
import scala.collection.mutable
import org.apache.spark.sql.types._


spark.conf.set("spark.sql.shuffle.partitions", "128")
spark.conf.set("spark.cassandra.input.consistency.level", "LOCAL_ONE")
spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")

spark.conf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127")

val Array(brokers, topics) = Array("ec2-54-85-62-208.compute-1.amazonaws.com:9092", "maxwell")

val timestampFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127"))

val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topics).option("failOnDataLoss", "false").load()

val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

// val tStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", "test").load()
// val tData = tStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2).distinct
// val q = tData.writeStream.outputMode("append").format("console").start

// var testIds = df.where("type='Test'").select("id")
// testIds.persist
// testIds.count

// val q = tData.writeStream.queryName("stids").outputMode("append").format("memory").start()
// sql("SELECT * FROM stids").show(false)


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
                          , is_ad_agency: Option[Int]
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
      , StructField("is_ad_agency", IntegerType)
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
      , StructField("is_ad_agency", IntegerType)
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

case class TblADCaccounts_salesrep_commissions(
                                                opType: String
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


case class TblCRMgeneric_product_credit(
                                         opType: String
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


// One time Update of Quotas
val quotas = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcquota", "keyspace" -> "adcentraldb")).load.repartition(1)
quotas.persist
quotas.count
quotas.createOrReplaceTempView("quotas")

val quotas_by_user_quarter = sql("SELECT year, quarter(to_date(CAST(unix_timestamp(CAST(month AS STRING), 'M') AS TIMESTAMP))) AS quarter, user_id, SUM(quota) AS quota FROM quotas GROUP BY 1, 2, 3")
quotas_by_user_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_user_quarter", "keyspace" -> "adcentraldb")).save

val quotas_by_quarter = sql("SELECT year, quarter(to_date(CAST(unix_timestamp(CAST(month AS STRING), 'M') AS TIMESTAMP))) AS quarter, SUM(quota) AS quota FROM quotas GROUP BY 1, 2")
quotas_by_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_quarter", "keyspace" -> "adcentraldb")).save


// Get Static Test Advertiser IDs
val staticTestAdvertiserIds = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "testadvertiserids", "keyspace" -> "adsystemdb")).load.repartition(1)
staticTestAdvertiserIds.persist
staticTestAdvertiserIds.count
staticTestAdvertiserIds.createOrReplaceTempView("staticTestAdvertiserIds")


// Get Static Exchange Rates
val statictblADScurrency_rates = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladscurrency_rates", "keyspace" -> "adsystemdb")).load.select("activity_date", "from_currency", "exchange_rate").distinct.repartition(1)
statictblADScurrency_rates.persist
statictblADScurrency_rates.count
statictblADScurrency_rates.createOrReplaceTempView("statictblADScurrency_rates")


val tblADCaccounts_salesrep_commissionsCT = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcaccounts_salesrep_commissions", "keyspace" -> "adcentraldb")).load.select(
  $"date"
  , $"salesrep_id".as("user_id")
  , $"advertiser_id"
  , $"newrevenue_jobsearch_millicents"
  , $"newrevenue_dradis_lifetime_millicents"
  , $"newrevenue_dradis_recurring_millicents"
  , $"newrevenue_resume_millicents"
  , $"revenue_jobsearch_millicents"
  , $"revenue_resume_millicents"
  , $"revenue_dradis_lifetime_millicents"
  , $"revenue_dradis_recurring_millicents"
).repartition(128)

tblADCaccounts_salesrep_commissionsCT.persist
tblADCaccounts_salesrep_commissionsCT.count
tblADCaccounts_salesrep_commissionsCT.createOrReplaceTempView("tblADCaccounts_salesrep_commissionsCT")


val tblCRMgeneric_product_creditCT = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tblcrmgeneric_product_credit", "keyspace" -> "adcentraldb")).load.select(
  $"activity_date".as("date")
  , $"user_id"
  , $"relationship"
  , $"currency"
  , $"rejected"
  , $"revenue_generic_product_local"
).repartition(128)

tblCRMgeneric_product_creditCT.persist
tblCRMgeneric_product_creditCT.count
tblCRMgeneric_product_creditCT.createOrReplaceTempView("tblCRMgeneric_product_creditCT")


val tblADCadvertiser_rep_revenuesCT = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcadvertiser_rep_revenues", "keyspace" -> "adcentraldb")).load.select(
  $"activity_date".as("date")
  , $"user_id"
  , $"advertiser_id"
  , $"relationship"
  , $"revenue_jobsearch_millicents"
  , $"revenue_resume_millicents"
).repartition(128)

tblADCadvertiser_rep_revenuesCT.persist
tblADCadvertiser_rep_revenuesCT.count
tblADCadvertiser_rep_revenuesCT.createOrReplaceTempView("tblADCadvertiser_rep_revenuesCT")


var query =
  """
    |SELECT 
    |      YEAR(date) AS year
    |     ,QUARTER(date) AS quarter
    |     ,user_id
    |     ,SUM(COALESCE(newrevenue_jobsearch_millicents, 0) + 
    |      COALESCE(newrevenue_dradis_lifetime_millicents, 0) + 
    |      COALESCE(newrevenue_dradis_recurring_millicents, 0) + 
    |      COALESCE(newrevenue_resume_millicents, 0)) AS sales_new_revenue
    |     ,SUM(COALESCE(revenue_jobsearch_millicents, 0) + 
    |      COALESCE(revenue_resume_millicents, 0) + 
    |      COALESCE(revenue_dradis_lifetime_millicents, 0) + 
    |      COALESCE(revenue_dradis_recurring_millicents, 0)) AS sales_revenue
    |FROM tblADCaccounts_salesrep_commissionsCT a
    |LEFT OUTER JOIN staticTestAdvertiserIds b 
    |ON a.advertiser_id = b.id
    |WHERE b.id IS NULL
    |GROUP BY 1, 2, 3
  """.stripMargin

val tblADCaccounts_salesrep_commissionsCTsr = sql(query)
tblADCaccounts_salesrep_commissionsCTsr.persist
tblADCaccounts_salesrep_commissionsCTsr.count
tblADCaccounts_salesrep_commissionsCTsr.createOrReplaceTempView("tblADCaccounts_salesrep_commissionsCTsr")


query =
  """
    |SELECT 
    |      YEAR(date) AS year
    |     ,QUARTER(date) AS quarter
    |     ,user_id
    |     ,SUM(CASE WHEN relationship IN ('SALES_REP', 'STRATEGIC_REP') THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END) AS sales_revenue
    |     ,SUM(CASE WHEN relationship = 'AGENCY_REP' THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END) AS agency_revenue
    |FROM
    |(SELECT 
    |      date
    |     ,user_id
    |     ,currency
    |     ,relationship
    |     ,COALESCE(revenue_generic_product_local, 0) AS total_local
    |FROM tblCRMgeneric_product_creditCT
    |WHERE relationship in ('SALES_REP', 'STRATEGIC_REP', 'AGENCY_REP') and rejected = 0
    |) arr 
    |INNER JOIN (SELECT * FROM statictblADScurrency_rates) er
    |ON arr.date = er.activity_date AND arr.currency = er.from_currency
    |GROUP BY 1, 2, 3
  """.stripMargin

val tblCRMgeneric_product_creditCTsr = sql(query)
tblCRMgeneric_product_creditCTsr.persist
tblCRMgeneric_product_creditCTsr.count
tblCRMgeneric_product_creditCTsr.createOrReplaceTempView("tblCRMgeneric_product_creditCTsr")


query =
  """
    |SELECT 
    |      YEAR(date) AS year
    |     ,QUARTER(date) AS quarter
    |     ,user_id
    |     ,SUM(CASE WHEN relationship = 'AGENCY_REP' THEN COALESCE(revenue_jobsearch_millicents, 0) + COALESCE(revenue_resume_millicents, 0) ELSE 0 END) AS agency_revenue
    |     ,SUM(CASE WHEN relationship = 'STRATEGIC_REP' THEN COALESCE(revenue_jobsearch_millicents, 0) + COALESCE(revenue_resume_millicents, 0) ELSE 0 END) AS strategic_revenue
    |FROM tblADCadvertiser_rep_revenuesCT a
    |LEFT OUTER JOIN
    |staticTestAdvertiserIds b
    |ON a.advertiser_id = b.id
    |WHERE b.id IS NULL
    |AND relationship IN ('AGENCY_REP', 'STRATEGIC_REP')
    |GROUP BY 1, 2, 3
  """.stripMargin

val tblADCadvertiser_rep_revenuesCTsr = sql(query)
tblADCadvertiser_rep_revenuesCTsr.persist
tblADCadvertiser_rep_revenuesCTsr.count
tblADCadvertiser_rep_revenuesCTsr.createOrReplaceTempView("tblADCadvertiser_rep_revenuesCTsr")


query =
  """
    |SELECT
    |      year
    |     ,quarter
    |     ,user_id
    |     ,sales_revenue
    |     ,agency_revenue
    |     ,sales_new_revenue
    |     ,strategic_revenue
    |     ,sales_revenue + agency_revenue + sales_new_revenue + strategic_revenue AS total_revenue
    |FROM
    |(
    |SELECT 
    |      COALESCE(a.year, b.year, c.year) AS year
    |     ,COALESCE(a.quarter, b.quarter, c.quarter) AS quarter
    |     ,COALESCE(a.user_id, b.user_id, c.user_id) AS user_id
    |     ,CAST(SUM(COALESCE(a.sales_revenue, 0) + COALESCE(b.sales_revenue, 0)) AS BIGINT) AS sales_revenue
    |     ,CAST(SUM(COALESCE(b.agency_revenue, 0) + COALESCE(c.agency_revenue, 0)) AS BIGINT) AS agency_revenue
    |     ,CAST(SUM(COALESCE(a.sales_new_revenue, 0)) AS BIGINT) AS sales_new_revenue
    |     ,CAST(SUM(COALESCE(c.strategic_revenue, 0)) AS BIGINT) AS strategic_revenue
    |FROM tblADCaccounts_salesrep_commissionsCTsr a
    |FULL OUTER JOIN
    |tblCRMgeneric_product_creditCTsr b ON a.year = b.year AND a.quarter = b.quarter AND a.user_id = b.user_id
    |FULL OUTER JOIN
    |tblADCadvertiser_rep_revenuesCTsr c ON a.year = c.year AND a.quarter = c.quarter AND a.user_id = c.user_id
    |GROUP BY 1, 2, 3
    |)
  """.stripMargin

val sales_revenue_summary_by_user_quarter = sql(query)
sales_revenue_summary_by_user_quarter.persist
sales_revenue_summary_by_user_quarter.count
sales_revenue_summary_by_user_quarter.createOrReplaceTempView("sales_revenue_summary_by_user_quarter")
sql("SELECT SUM(sales_revenue) + SUM(agency_revenue) + SUM(strategic_revenue) + SUM(sales_new_revenue) AS total_revenue, SUM(sales_revenue), SUM(agency_revenue), SUM(strategic_revenue), SUM(sales_new_revenue) FROM sales_revenue_summary_by_user_quarter WHERE year = 2018 and quarter = 3").show(false)

sales_revenue_summary_by_user_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_summary_by_user_quarter", "keyspace" -> "adcentraldb")).save
sales_revenue_summary_by_user_quarter.select("year", "quarter", "user_id", "total_revenue").write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_user_quarter", "keyspace" -> "adcentraldb")).save


query =
  """
    |SELECT
    |      year
    |     ,quarter
    |     ,SUM(sales_revenue) AS sales_revenue
    |     ,SUM(agency_revenue) AS agency_revenue
    |     ,SUM(sales_new_revenue) AS sales_new_revenue
    |     ,SUM(strategic_revenue) AS strategic_revenue
    |     ,SUM(total_revenue) AS total_revenue
    |FROM sales_revenue_summary_by_user_quarter
    |GROUP BY 1, 2
  """.stripMargin

val sales_revenue_summary_by_quarter = sql(query)
sales_revenue_summary_by_quarter.persist
sales_revenue_summary_by_quarter.count
sales_revenue_summary_by_quarter.createOrReplaceTempView("sales_revenue_summary_by_quarter")
sales_revenue_summary_by_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_summary_by_quarter", "keyspace" -> "adcentraldb")).save
sales_revenue_summary_by_quarter.select("year", "quarter", "total_revenue").write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_quarter", "keyspace" -> "adcentraldb")).save


// Streaming Data Starts from here
spark.conf.set("spark.sql.shuffle.partitions", "1")

case class Sales_Revenue_Summary(year: Int, quarter: Int, user_id: BigInt, sales_revenue: BigInt, agency_revenue: BigInt, strategic_revenue: BigInt, sales_new_revenue: BigInt)

val Sales_Revenue_SummaryWriter = new ForeachWriter[Sales_Revenue_Summary] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: Sales_Revenue_Summary): Unit = {
    val total_revenue = value.sales_revenue + value.agency_revenue + value.strategic_revenue + value.sales_new_revenue
    val cQuery1 = s"update adcentraldb.sales_revenue_summary_by_user_quarter SET total_revenue = total_revenue + ${total_revenue}, sales_revenue = sales_revenue + ${value.sales_revenue}, agency_revenue = agency_revenue + ${value.agency_revenue}, strategic_revenue = strategic_revenue + ${value.strategic_revenue}, sales_new_revenue = sales_new_revenue + ${value.sales_new_revenue} WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}"
    val cQuery2 = s"update adcentraldb.sales_revenue_summary_by_quarter SET total_revenue = total_revenue + ${total_revenue}, sales_revenue = sales_revenue + ${value.sales_revenue}, agency_revenue = agency_revenue + ${value.agency_revenue}, strategic_revenue = strategic_revenue + ${value.strategic_revenue}, sales_new_revenue = sales_new_revenue + ${value.sales_new_revenue} WHERE year = ${value.year} AND quarter = ${value.quarter}"
    val cQuery3 = s"SELECT total_revenue FROM adcentraldb.sales_revenue_summary_by_user_quarter WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}"
    val cQuery4 = s"SELECT total_revenue FROM adcentraldb.sales_revenue_summary_by_quarter WHERE year = ${value.year} AND quarter = ${value.quarter}"

    connector.withSessionDo { session =>
      session.execute(cQuery1)
      session.execute(cQuery2)
      val cRow1 = session.execute(cQuery3).one
      val total_revenue_user_quarter = if (cRow1 != null && cRow1.getObject("total_revenue") != null) BigInt(cRow1.getObject("total_revenue").toString) else BigInt(0)
      session.execute(s"update adcentraldb.sales_revenue_quota_summary_by_user_quarter SET total_revenue = ${total_revenue_user_quarter} WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}")

      val cRow2 = session.execute(cQuery4).one
      val total_revenue_quarter = if (cRow2 != null && cRow2.getObject("total_revenue") != null) BigInt(cRow2.getObject("total_revenue").toString) else BigInt(0)
      session.execute(s"update adcentraldb.sales_revenue_quota_summary_by_quarter SET total_revenue = ${total_revenue_quarter} WHERE year = ${value.year} AND quarter = ${value.quarter}")

    }
  }

  def close(errorOrNull: Throwable): Unit = {}
}



// Get Streaming Test Advertiser IDs
val tblAdvertiser = rawData.select(from_json($"value", Tbladvertiser.jsonSchema).as("value")).filter($"value.table" === "tbladvertiser").select($"value.type".as("opType"), $"value.data.*")
val streamTestAdvertiserIds = tblAdvertiser.where("opType IN ('insert', 'update', 'delete') AND type='Test'").selectExpr("CAST(id AS BIGINT)").distinct

// Construct inmemory table of these test advertiser ids
val streamTestAdvertiserIdsQuery = streamTestAdvertiserIds.writeStream.queryName("streamTestAdvertiserIds").outputMode("append").format("memory").start()
sql("SELECT * FROM streamTestAdvertiserIds").show(50, false)

// Get Streaming Exchange Rates
val tblADScurrency_rates = rawData.select(from_json($"value", TblADScurrency_rates.jsonSchema).as("value")).filter($"value.table" === "tblADScurrency_rates").select($"value.type".as("opType"), $"value.data.*")
val streamtblADScurrency_rates = tblADScurrency_rates.where("opType IN ('insert', 'update')").select("activity_date", "from_currency", "exchange_rate").distinct

// Construct inmemory table of these stream tblADScurrency_rates
val streamtblADScurrency_ratesQuery = streamtblADScurrency_rates.writeStream.queryName("streamtblADScurrency_rates").outputMode("append").format("memory").start()
sql("SELECT * FROM streamtblADScurrency_rates").show(50, false)


// Construct Reveneue Summary for tblADCaccounts_salesrep_commissions
val tblADCaccounts_salesrep_commissions = rawData.select(from_json($"value", TblADCaccounts_salesrep_commissions.jsonSchema).as("value")).filter($"value.table" === "tblADCaccounts_salesrep_commissions").select($"value.type".as("opType")
  , $"value.data.date"
  , $"value.data.salesrep_id".as("user_id")
  , $"value.data.advertiser_id"
  , $"value.data.newrevenue_jobsearch_millicents"
  , $"value.data.newrevenue_dradis_lifetime_millicents"
  , $"value.data.newrevenue_dradis_recurring_millicents"
  , $"value.data.newrevenue_resume_millicents"
  , $"value.data.revenue_jobsearch_millicents"
  , $"value.data.revenue_resume_millicents"
  , $"value.data.revenue_dradis_lifetime_millicents"
  , $"value.data.revenue_dradis_recurring_millicents"
  , $"value.old.newrevenue_jobsearch_millicents".as("old_newrevenue_jobsearch_millicents")
  , $"value.old.newrevenue_dradis_lifetime_millicents".as("old_newrevenue_dradis_lifetime_millicents")
  , $"value.old.newrevenue_dradis_recurring_millicents".as("old_newrevenue_dradis_recurring_millicents")
  , $"value.old.newrevenue_resume_millicents".as("old_newrevenue_resume_millicents")
  , $"value.old.revenue_jobsearch_millicents".as("old_revenue_jobsearch_millicents")
  , $"value.old.revenue_resume_millicents".as("old_revenue_resume_millicents")
  , $"value.old.revenue_dradis_lifetime_millicents".as("old_revenue_dradis_lifetime_millicents")
  , $"value.old.revenue_dradis_recurring_millicents".as("old_revenue_dradis_recurring_millicents")).where("opType IN ('insert', 'update', 'delete')")

tblADCaccounts_salesrep_commissions.createOrReplaceTempView("tblADCaccounts_salesrep_commissions")

val tblADCaccounts_salesrep_commissionsSummarySql =
  """
    |SELECT 
    |      YEAR(date) AS year
    |     ,QUARTER(date) AS quarter
    |     ,user_id
    |     ,CASE WHEN opType = 'insert' THEN 
    |                                       COALESCE(revenue_jobsearch_millicents, 0) + 
    |                                       COALESCE(revenue_resume_millicents, 0) + 
    |                                       COALESCE(revenue_dradis_lifetime_millicents, 0) + 
    |                                       COALESCE(revenue_dradis_recurring_millicents, 0)    
    |           WHEN opType = 'update' THEN 
    |                                       (COALESCE(revenue_jobsearch_millicents, 0 ) - COALESCE(old_revenue_jobsearch_millicents, revenue_jobsearch_millicents)) + 
    |                                       (COALESCE(revenue_resume_millicents, 0 ) - COALESCE(old_revenue_resume_millicents, revenue_resume_millicents)) + 
    |                                       (COALESCE(revenue_dradis_lifetime_millicents, 0 ) - COALESCE(old_revenue_dradis_lifetime_millicents, revenue_dradis_lifetime_millicents)) + 
    |                                       (COALESCE(revenue_dradis_recurring_millicents, 0 ) - COALESCE(old_revenue_dradis_recurring_millicents, revenue_dradis_recurring_millicents))
    |           ELSE 
    |                                       -(COALESCE(revenue_jobsearch_millicents, 0) + 
    |                                       COALESCE(revenue_resume_millicents, 0) + 
    |                                       COALESCE(revenue_dradis_lifetime_millicents, 0) + 
    |                                       COALESCE(revenue_dradis_recurring_millicents, 0))
    |           END AS sales_revenue
    |     ,0 AS agency_revenue
    |     ,0 AS strategic_revenue
    |     ,CASE WHEN opType = 'insert' THEN 
    |                                       COALESCE(newrevenue_jobsearch_millicents, 0) + 
    |                                       COALESCE(newrevenue_dradis_lifetime_millicents, 0) + 
    |                                       COALESCE(newrevenue_dradis_recurring_millicents, 0) + 
    |                                       COALESCE(newrevenue_resume_millicents, 0)    
    |           WHEN opType = 'update' THEN 
    |                                       (COALESCE(newrevenue_jobsearch_millicents, 0 ) - COALESCE(old_newrevenue_jobsearch_millicents, newrevenue_jobsearch_millicents)) + 
    |                                       (COALESCE(newrevenue_dradis_lifetime_millicents, 0 ) - COALESCE(old_newrevenue_dradis_lifetime_millicents, newrevenue_dradis_lifetime_millicents)) + 
    |                                       (COALESCE(newrevenue_dradis_recurring_millicents, 0 ) - COALESCE(old_newrevenue_dradis_recurring_millicents, newrevenue_dradis_recurring_millicents)) + 
    |                                       (COALESCE(newrevenue_resume_millicents, 0 ) - COALESCE(old_newrevenue_resume_millicents, newrevenue_resume_millicents))
    |           ELSE 
    |                                       -(COALESCE(newrevenue_jobsearch_millicents, 0) + 
    |                                       COALESCE(newrevenue_dradis_lifetime_millicents, 0) + 
    |                                       COALESCE(newrevenue_dradis_recurring_millicents, 0) + 
    |                                       COALESCE(newrevenue_resume_millicents, 0))
    |           END AS sales_new_revenue
    |FROM tblADCaccounts_salesrep_commissions
    |WHERE advertiser_id NOT IN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds)
  """.stripMargin

val tblADCaccounts_salesrep_commissionsSummaryQuery = sql(tblADCaccounts_salesrep_commissionsSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()


// Construct Reveneue Summary for tblCRMgeneric_product_credit
val tblCRMgeneric_product_credit = rawData.select(from_json($"value", TblCRMgeneric_product_credit.jsonSchema).as("value")).filter($"value.table" === "tblCRMgeneric_product_credit").select($"value.type".as("opType")
  , $"value.data.activity_date".as("date")
  , $"value.data.user_id"
  , $"value.data.relationship"
  , $"value.data.currency"
  , $"value.data.rejected"
  , $"value.data.revenue_generic_product_local"
  , $"value.old.revenue_generic_product_local".as("old_revenue_generic_product_local")).where("opType IN ('insert', 'update', 'delete')")

tblCRMgeneric_product_credit.createOrReplaceTempView("tblCRMgeneric_product_credit")

val tblCRMgeneric_product_creditSummarySql =
  """
    |SELECT 
    |      YEAR(date) AS year
    |     ,QUARTER(date) AS quarter
    |     ,user_id
    |     ,CASE WHEN relationship IN ('SALES_REP', 'STRATEGIC_REP') THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END AS sales_revenue
    |     ,CASE WHEN relationship = 'AGENCY_REP' THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END AS agency_revenue
    |     ,0 AS strategic_revenue
    |     ,0 AS sales_new_revenue
    |FROM
    |(SELECT 
    |      date
    |     ,user_id
    |     ,currency
    |     ,relationship
    |     ,CASE WHEN opType = 'insert' THEN COALESCE(revenue_generic_product_local, 0)
    |           WHEN opType = 'update' THEN COALESCE(revenue_generic_product_local, 0 ) - COALESCE(old_revenue_generic_product_local, revenue_generic_product_local)
    |           ELSE -COALESCE(revenue_generic_product_local, 0)
    |           END AS total_local
    |FROM tblCRMgeneric_product_credit
    |WHERE relationship in ('SALES_REP', 'STRATEGIC_REP', 'AGENCY_REP') and rejected = 0
    |) arr 
    |INNER JOIN (SELECT * FROM statictblADScurrency_rates UNION SELECT * FROM streamtblADScurrency_rates) er
    |ON arr.date = er.activity_date AND arr.currency = er.from_currency
  """.stripMargin

val tblCRMgeneric_product_creditSummaryQuery = sql(tblCRMgeneric_product_creditSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()


// Construct Reveneue Summary for tblADCadvertiser_rep_revenues
val tblADCadvertiser_rep_revenues = rawData.select(from_json($"value", TblADCadvertiser_rep_revenues.jsonSchema).as("value")).filter($"value.table" === "tblADCadvertiser_rep_revenues").select($"value.type".as("opType")
  , $"value.data.activity_date".as("date")
  , $"value.data.user_id"
  , $"value.data.advertiser_id"
  , $"value.data.relationship"
  , $"value.data.revenue_jobsearch_millicents"
  , $"value.data.revenue_resume_millicents"
  , $"value.old.revenue_jobsearch_millicents".as("old_revenue_jobsearch_millicents")
  , $"value.old.revenue_resume_millicents".as("old_revenue_resume_millicents")).where("opType IN ('insert', 'update', 'delete')")

tblADCadvertiser_rep_revenues.createOrReplaceTempView("tblADCadvertiser_rep_revenues")

val tblADCadvertiser_rep_revenuesSummarySql =
  """
    |SELECT 
    |      year
    |     ,quarter
    |     ,user_id
    |     ,0 AS sales_revenue
    |     ,CASE WHEN relationship = 'AGENCY_REP' THEN revenue ELSE 0 END AS agency_revenue
    |     ,CASE WHEN relationship = 'STRATEGIC_REP' THEN revenue ELSE 0 END AS strategic_revenue
    |     ,0 AS sales_new_revenue
    |FROM 
    |(
    |SELECT 
    |      YEAR(date) AS year
    |     ,QUARTER(date) AS quarter
    |     ,user_id
    |     ,relationship
    |     ,CASE WHEN opType = 'insert' THEN 
    |                                       COALESCE(revenue_jobsearch_millicents, 0) + COALESCE(revenue_resume_millicents, 0)
    |           WHEN opType = 'update' THEN 
    |                                       (COALESCE(revenue_jobsearch_millicents, 0 ) - COALESCE(old_revenue_jobsearch_millicents, revenue_jobsearch_millicents)) + 
    |                                       (COALESCE(revenue_resume_millicents, 0 ) - COALESCE(old_revenue_resume_millicents, revenue_resume_millicents))
    |           ELSE 
    |                                       -(COALESCE(revenue_jobsearch_millicents, 0) + COALESCE(revenue_resume_millicents, 0))
    |           END AS revenue
    |FROM tblADCadvertiser_rep_revenues
    |WHERE advertiser_id NOT IN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds)
    |AND relationship IN ('AGENCY_REP', 'STRATEGIC_REP')
    |)
  """.stripMargin

val tblADCadvertiser_rep_revenuesSummaryQuery = sql(tblADCadvertiser_rep_revenuesSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()


streamTestAdvertiserIdsQuery.status
streamtblADScurrency_ratesQuery.status
tblADCaccounts_salesrep_commissionsSummaryQuery.status
tblADCadvertiser_rep_revenuesSummaryQuery.status
tblCRMgeneric_product_creditSummaryQuery.status

tblADCaccounts_salesrep_commissionsSummaryQuery.stop
tblADCadvertiser_rep_revenuesSummaryQuery.stop
tblCRMgeneric_product_creditSummaryQuery.stop


val sales_revenue_quota_summary_by_quarter = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "sales_revenue_quota_summary_by_quarter", "keyspace" -> "adcentraldb")).load
sales_revenue_quota_summary_by_quarter.persist
sales_revenue_quota_summary_by_quarter.count
sales_revenue_quota_summary_by_quarter.createOrReplaceTempView("sales_revenue_quota_summary_by_quarter")
sql("SELECT * FROM sales_revenue_quota_summary_by_quarter WHERE year = 2018 and quarter = 3").show(false)
sql("SELECT SUM(sales_revenue), SUM(agency_revenue), SUM(strategic_revenue), SUM(sales_new_revenue) FROM sales_revenue_summary_by_user_quarter WHERE year = 2018 and quarter = 3").show(false)
SELECT SUM (sales_revenue), SUM(agency_revenue), SUM(strategic_revenue), SUM(sales_new_revenue) FROM adcentraldb.sales_revenue_summary_by_user_quarter WHERE year = 2018 and quarter = 3;
SELECT SUM (sales_revenue), SUM(agency_revenue), SUM(strategic_revenue), SUM(sales_new_revenue) FROM adcentraldb.sales_revenue_summary_by_quarter WHERE year = 2018 and quarter = 3;


