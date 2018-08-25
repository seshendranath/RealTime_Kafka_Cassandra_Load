// One Time Initial Dump to Cassandra in Spark Shell
import scala.collection.mutable
import org.apache.spark.sql._
import com.datastax.driver.core._

def sparkToCassandraDataType(dataType: String): String = {
  val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

  dataType match {
    case "StringType" => "text"
    case "LongType" => "bigint"
    case "DoubleType" => "double"
    case "IntegerType" => "int"
    case "DateType" => "date"
    case "TimestampType" => "timestamp"
    case "BooleanType" => "boolean"
    case decimalPattern() => "decimal" // "double"// dataType.replace("Type", "").toLowerCase
    case _ => "text"
  }
}

def getColsFromDF(df: DataFrame, exclude: Seq[String] = Seq()): Array[(String, String)] = {
  val cols = mutable.ArrayBuffer[(String, String)]()
  for (column <- df.dtypes) {
    val (col, dataType) = column
    if (!(exclude contains col)) {
      cols += ((col, sparkToCassandraDataType(dataType)))
    }
  }
  cols.toArray
}

val cluster = Cluster.builder().addContactPoint("172.31.31.252").build()
val session = cluster.connect("adcentraldb")

spark.conf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127")
spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")

val db = "adsystemdb"
val table = "tblADScurrency_rates"
val pk = "date,advertiser_id"

val df = spark.read.parquet(s"s3a://indeed-data/datalake/v1/prod/mysql/$db/$table")
df1.unpersist
val df1 = df.repartition(160)
df1.persist
df1.count

val cols = getColsFromDF(df)


val query = s"CREATE TABLE IF NOT EXISTS $db.$table (" + cols.map(x => x._1 + " " + x._2).mkString(",") + s", PRIMARY KEY ($pk));"
session.execute(query)

var df2 = df1
for (c <- df1.columns) df2 = df2.withColumnRenamed(c, c.toLowerCase)
df2.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> table.toLowerCase, "keyspace" -> db)).save


// Testing Real Time Cassandra Loads in Spark Shell
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core._
import scala.collection.mutable
import org.apache.spark.sql.types._

// import com.fasterxml.jackson.databind.ObjectMapper
// import com.fasterxml.jackson.module.scala.DefaultScalaModule
// import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
// import org.json4s._
// import org.json4s.jackson.JsonMethods._

// implicit val formats: DefaultFormats = DefaultFormats
// val mapper = new ObjectMapper() with ScalaObjectMapper
// mapper.registerModule(DefaultScalaModule)


spark.conf.set("spark.sql.shuffle.partitions", "10")
spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
val Array(brokers, topics) = Array("ec2-54-85-62-208.compute-1.amazonaws.com:9092", "maxwell")

val timestampFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127"))

val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topics).load()
  //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)

val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

// val qq = rawData.writeStream.outputMode("append").format("console").start
// qq.stop


case class TblAdvertiser(   opType: String
                          , id: Option[Int]
                          , account_id: Option[Int]
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
                          , date_created: String
                          , active: Option[Int]
                          , ip_address: String
                          , referral_id: Option[Int]
                          , monthly_budget: Option[Double]
                          , expended_budget: Option[Double]
                          , `type`: String
                          , advertiser_number: String
                          , show_conversions: Option[Int]
                          , billing_threshold: Option[Double]
                          , payment_method: String
                          , industry: String
                          , agency_discount: Option[Double]
                          , is_ad_agency: Option[Int]
                          , process_level: String
                          , estimated_budget: Option[Double]
                          , first_revenue_date: String
                          , last_revenue_date: String
                          , first_revenue_override: String
                          , terms: String
                          , currency: String
                          , monthly_budget_local: Option[BigInt]
                          , expended_budget_local: Option[BigInt]
                          , billing_threshold_local: Option[BigInt]
                          , estimated_budget_local: Option[BigInt]
                          , employee_count: String
                          , last_updated: String
                        )

val tblAdvertiserSchema = StructType(Array(
 StructField("database", StringType),
 StructField("table", StringType),
 StructField("type", StringType),
 StructField("ts", TimestampType),
 StructField("position", StringType),
 StructField("primary_key", StructType(Array(StructField("date", DateType), StructField("advertiser_id", IntegerType)))),
 StructField("data",StructType(Array(
                    StructField("id",  IntegerType),
                    StructField("account_id",  IntegerType),
                    StructField("company",  StringType),
                    StructField("contact",  StringType),
                    StructField("url",  StringType),
                    StructField("address1",  StringType),
                    StructField("address2",  StringType),
                    StructField("city",  StringType),
                    StructField("state",  StringType),
                    StructField("zip",  StringType),
                    StructField("phone",  StringType),
                    StructField("phone_type",  StringType),
                    StructField("verified_phone",  StringType),
                    StructField("verified_phone_extension",  StringType),
                    StructField("uuidstring",  StringType),
                    StructField("date_created",  StringType),
                    StructField("active",  IntegerType),
                    StructField("ip_address",  StringType),
                    StructField("referral_id",  IntegerType),
                    StructField("monthly_budget",  DoubleType),
                    StructField("expended_budget",  DoubleType),
                    StructField("type",  StringType),
                    StructField("advertiser_number",  StringType),
                    StructField("show_conversions",  IntegerType),
                    StructField("billing_threshold",  DoubleType),
                    StructField("payment_method",  StringType),
                    StructField("industry",  StringType),
                    StructField("agency_discount",  DoubleType),
                    StructField("is_ad_agency",  IntegerType),
                    StructField("process_level",  StringType),
                    StructField("estimated_budget",  DoubleType),
                    StructField("first_revenue_date",  StringType),
                    StructField("last_revenue_date",  StringType),
                    StructField("first_revenue_override",  StringType),
                    StructField("terms",  StringType),
                    StructField("currency",  StringType),
                    StructField("monthly_budget_local",  LongType),
                    StructField("expended_budget_local",  LongType),
                    StructField("billing_threshold_local",  LongType),
                    StructField("estimated_budget_local",  LongType),
                    StructField("employee_count",  StringType),
                    StructField("last_updated",  StringType)
          ))),
 StructField("old", StructType(Array(
                 StructField("company",  StringType),
                 StructField("contact",  StringType),
                 StructField("url",  StringType),
                 StructField("address1",  StringType),
                 StructField("address2",  StringType),
                 StructField("city",  StringType),
                 StructField("state",  StringType),
                 StructField("zip",  StringType),
                 StructField("phone",  StringType),
                 StructField("phone_type",  StringType),
                 StructField("verified_phone",  StringType),
                 StructField("verified_phone_extension",  StringType),
                 StructField("uuidstring",  StringType),
                 StructField("date_created",  StringType),
                 StructField("active",  IntegerType),
                 StructField("ip_address",  StringType),
                 StructField("referral_id",  IntegerType),
                 StructField("monthly_budget",  DoubleType),
                 StructField("expended_budget",  DoubleType),
                 StructField("type",  StringType),
                 StructField("advertiser_number",  StringType),
                 StructField("show_conversions",  IntegerType),
                 StructField("billing_threshold",  DoubleType),
                 StructField("payment_method",  StringType),
                 StructField("industry",  StringType),
                 StructField("agency_discount",  DoubleType),
                 StructField("is_ad_agency",  IntegerType),
                 StructField("process_level",  StringType),
                 StructField("estimated_budget",  DoubleType),
                 StructField("first_revenue_date",  StringType),
                 StructField("last_revenue_date",  StringType),
                 StructField("first_revenue_override",  StringType),
                 StructField("terms",  StringType),
                 StructField("currency",  StringType),
                 StructField("monthly_budget_local",  LongType),
                 StructField("expended_budget_local",  LongType),
                 StructField("billing_threshold_local",  LongType),
                 StructField("estimated_budget_local",  LongType),
                 StructField("employee_count",  StringType),
                 StructField("last_updated",  StringType)
          )))
))

val tblAdvertiser = rawData.select(from_json($"value", tblAdvertiserSchema).as("value")).filter($"value.table" === "tbladvertiser").select($"value.type".as("opType"), $"value.data.*")
// val q = tblAdvertiser.as[TblAdvertiser].writeStream.outputMode("append").format("console").start
// q.stop

tblAdvertiser.createOrReplaceTempView("tblAdvertiser")


val tblAdvertiserWriter = new ForeachWriter[TblAdvertiser] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: TblAdvertiser): Unit = {

    def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

    if (value.opType == "insert" || value.opType == "update")
      {
        val cQuery1 =
                s"""
                  |INSERT INTO adsystemdb.tbladvertiser (id,account_id,company,contact,url,address1,address2,city,state,zip,phone,phone_type,verified_phone,verified_phone_extension,uuidstring,date_created,active,ip_address,referral_id,monthly_budget,expended_budget,type,advertiser_number,show_conversions,billing_threshold,payment_method,industry,agency_discount,is_ad_agency,process_level,estimated_budget,first_revenue_date,last_revenue_date,first_revenue_override,terms,currency,monthly_budget_local,expended_budget_local,billing_threshold_local,estimated_budget_local,employee_count,last_updated)
                  |VALUES (
                  |${value.id.getOrElse(null)}
                  |,${value.account_id.getOrElse(null)}
                  |,${if (value.company == null) null else "'" + value.company.replaceAll("'", "''")+ "'"}
                  |,${if (value.contact== null) null else "'" + value.contact.replaceAll("'", "''")+ "'"}
                  |,${if (value.url== null) null else "'" + value.url.replaceAll("'", "''")+ "'"}
                  |,${if (value.address1== null) null else "'" + value.address1.replaceAll("'", "''")+ "'"}
                  |,${if (value.address2== null) null else "'" + value.address2.replaceAll("'", "''")+ "'"}
                  |,${if (value.city== null) null else "'" + value.city.replaceAll("'", "''")+ "'"}
                  |,${if (value.state== null) null else "'" + value.state.replaceAll("'", "''")+ "'"}
                  |,${if (value.zip== null) null else "'" + value.zip.replaceAll("'", "''")+ "'"}
                  |,${if (value.phone== null) null else "'" + value.phone.replaceAll("'", "''")+ "'"}
                  |,${if (value.phone_type== null) null else "'" + value.phone_type.replaceAll("'", "''")+ "'"}
                  |,${if (value.verified_phone== null) null else "'" + value.verified_phone.replaceAll("'", "''")+ "'"}
                  |,${if (value.verified_phone_extension== null) null else "'" + value.verified_phone_extension.replaceAll("'", "''")+ "'"}
                  |,${if (value.uuidstring== null) null else "'" + value.uuidstring.replaceAll("'", "''")+ "'"}
                  |,${if (value.date_created== null) null else "'" + value.date_created.replaceAll("'", "''")+ "'"}
                  |,${value.active.getOrElse(null)}
                  |,${if (value.ip_address== null) null else "'" + value.ip_address.replaceAll("'", "''")+ "'"}
                  |,${value.referral_id.getOrElse(null)}
                  |,${value.monthly_budget.getOrElse(null)}
                  |,${value.expended_budget.getOrElse(null)}
                  |,${if (value.`type`== null) null else "'" + value.`type`.replaceAll("'", "''")+ "'"}
                  |,${if (value.advertiser_number== null) null else "'" + value.advertiser_number.replaceAll("'", "''")+ "'"}
                  |,${value.show_conversions.getOrElse(null)}
                  |,${value.billing_threshold.getOrElse(null)}
                  |,${if (value.payment_method== null) null else "'" + value.payment_method.replaceAll("'", "''")+ "'"}
                  |,${if (value.industry== null) null else "'" + value.industry.replaceAll("'", "''")+ "'"}
                  |,${value.agency_discount.getOrElse(null)}
                  |,${intBool(value.is_ad_agency.getOrElse(null))}
                  |,${if (value.process_level== null) null else "'" + value.process_level.replaceAll("'", "''")+ "'"}
                  |,${value.estimated_budget.getOrElse(null)}
                  |,${if (value.first_revenue_date== null) null else "'" + value.first_revenue_date.replaceAll("'", "''")+ "'"}
                  |,${if (value.last_revenue_date== null) null else "'" + value.last_revenue_date.replaceAll("'", "''")+ "'"}
                  |,${if (value.first_revenue_override== null) null else "'" + value.first_revenue_override.replaceAll("'", "''")+ "'"}
                  |,${if (value.terms== null) null else "'" + value.terms.replaceAll("'", "''")+ "'"}
                  |,${if (value.currency== null) null else "'" + value.currency.replaceAll("'", "''")+ "'"}
                  |,${value.monthly_budget_local.getOrElse(null)}
                  |,${value.expended_budget_local.getOrElse(null)}
                  |,${value.billing_threshold_local.getOrElse(null)}
                  |,${value.estimated_budget_local.getOrElse(null)}
                  |,${if (value.employee_count== null) null else "'" + value.employee_count.replaceAll("'", "''")+ "'"}
                  |,${if (value.last_updated== null) null else "'" + value.last_updated.replaceAll("'", "''")+ "'"}
                  |)
                """.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
  }
  def close(errorOrNull: Throwable): Unit = {}
}


val streamQuery = tblAdvertiser.as[TblAdvertiser].writeStream.foreach(tblAdvertiserWriter).outputMode("append").start
streamQuery.stop



case class TblADCaccounts_salesrep_commissions  (
                opType: String
               ,date: String
               ,advertiser_id: Option[Int]
               ,salesrep_id: Option[Int]
               ,revenue_jobsearch_millicents: Option[BigInt]
               ,revenue_dradis_lifetime_millicents: Option[BigInt]
               ,revenue_dradis_recurring_millicents: Option[BigInt]
               ,revenue_resume_millicents: Option[BigInt]
               ,revenue_ineligible_millicents: Option[BigInt]
               ,revenue_jobsearch_local: Option[Int]
               ,revenue_dradis_lifetime_local: Option[Int]
               ,revenue_dradis_recurring_local: Option[Int]
               ,revenue_resume_local: Option[Int]
               ,revenue_ineligible_local: Option[Int]
               ,discount_local: Option[Int]
               ,discount_forward_local: Option[Int]
               ,discounted_revenue_local: Option[Int]
               ,commission_rate: Option[Double]
               ,commission_amount_local: Option[Int]
               ,commission_amount_millicents: Option[BigInt]
               ,newrevenue_jobsearch_millicents: Option[BigInt]
               ,newrevenue_dradis_lifetime_millicents: Option[BigInt]
               ,newrevenue_dradis_recurring_millicents: Option[BigInt]
               ,newrevenue_resume_millicents: Option[BigInt]
               ,currency: String
               ,date_modified: String)


val tblADCaccounts_salesrep_commissionsSchema = StructType(Array(
 StructField("database", StringType),
 StructField("table", StringType),
 StructField("type", StringType),
 StructField("ts", TimestampType),
 StructField("position", StringType),
 StructField("primary_key", StructType(Array(StructField("date", DateType), StructField("advertiser_id", IntegerType)))),
 StructField("data", StructType(Array(
                 StructField("date", DateType),
                 StructField("advertiser_id", IntegerType),
                 StructField("salesrep_id", IntegerType),
                 StructField("revenue_jobsearch_millicents", LongType),
                 StructField("revenue_dradis_lifetime_millicents", LongType),
                 StructField("revenue_dradis_recurring_millicents", LongType),
                 StructField("revenue_resume_millicents", LongType),
                 StructField("revenue_ineligible_millicents", LongType),
                 StructField("revenue_jobsearch_local", IntegerType),
                 StructField("revenue_dradis_lifetime_local", IntegerType),
                 StructField("revenue_dradis_recurring_local", IntegerType),
                 StructField("revenue_resume_local", IntegerType),
                 StructField("revenue_ineligible_local", IntegerType),
                 StructField("discount_local", IntegerType),
                 StructField("discount_forward_local", IntegerType),
                 StructField("discounted_revenue_local", IntegerType),
                 StructField("commission_rate", DoubleType),
                 StructField("commission_amount_local", IntegerType),
                 StructField("commission_amount_millicents", LongType),
                 StructField("newrevenue_jobsearch_millicents", LongType),
                 StructField("newrevenue_dradis_lifetime_millicents", LongType),
                 StructField("newrevenue_dradis_recurring_millicents", LongType),
                 StructField("newrevenue_resume_millicents", LongType),
                 StructField("currency", StringType),
                 StructField("date_modified", TimestampType)
          ))),
 StructField("old", StructType(Array(
                 StructField("date", DateType),
                 StructField("advertiser_id", IntegerType),
                 StructField("salesrep_id", IntegerType),
                 StructField("revenue_jobsearch_millicents", LongType),
                 StructField("revenue_dradis_lifetime_millicents", LongType),
                 StructField("revenue_dradis_recurring_millicents", LongType),
                 StructField("revenue_resume_millicents", LongType),
                 StructField("revenue_ineligible_millicents", LongType),
                 StructField("revenue_jobsearch_local", IntegerType),
                 StructField("revenue_dradis_lifetime_local", IntegerType),
                 StructField("revenue_dradis_recurring_local", IntegerType),
                 StructField("revenue_resume_local", IntegerType),
                 StructField("revenue_ineligible_local", IntegerType),
                 StructField("discount_local", IntegerType),
                 StructField("discount_forward_local", IntegerType),
                 StructField("discounted_revenue_local", IntegerType),
                 StructField("commission_rate", DoubleType),
                 StructField("commission_amount_local", IntegerType),
                 StructField("commission_amount_millicents", LongType),
                 StructField("newrevenue_jobsearch_millicents", LongType),
                 StructField("newrevenue_dradis_lifetime_millicents", LongType),
                 StructField("newrevenue_dradis_recurring_millicents", LongType),
                 StructField("newrevenue_resume_millicents", LongType),
                 StructField("currency", StringType),
                 StructField("date_modified", TimestampType)
          )))
))

val tblADCaccounts_salesrep_commissions = rawData.select(from_json($"value", tblADCaccounts_salesrep_commissionsSchema).as("value")).filter($"value.table" === "tblADCaccounts_salesrep_commissions").select($"value.type".as("opType"), $"value.data.*")
// val q = tblAdvertiser.as[TblAdvertiser].writeStream.outputMode("append").format("console").start
// q.stop

tblADCaccounts_salesrep_commissions.createOrReplaceTempView("tblADCaccounts_salesrep_commissions")

tblADCaccounts_salesrep_commissions.dtypes.map{case (col, dtype) => if (Set("StringType", "DateType", "TimestampType") contains dtype) s"""$${if (value.$col == null) null else "'" + value.$col.replaceAll("'", "''")+ "'"}""" else s"""$${value.$col.getOrElse(null)}"""}.foreach(println)

val tblADCaccounts_salesrep_commissionsWriter = new ForeachWriter[TblADCaccounts_salesrep_commissions] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: TblADCaccounts_salesrep_commissions): Unit = {

    def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

    if (value.opType == "insert" || value.opType == "update")
      {
        val cQuery1 =
                s"""
                  |INSERT INTO adcentraldb.tbladcaccounts_salesrep_commissions (date,advertiser_id,salesrep_id,revenue_jobsearch_millicents,revenue_dradis_lifetime_millicents,revenue_dradis_recurring_millicents,revenue_resume_millicents,revenue_ineligible_millicents,revenue_jobsearch_local,revenue_dradis_lifetime_local,revenue_dradis_recurring_local,revenue_resume_local,revenue_ineligible_local,discount_local,discount_forward_local,discounted_revenue_local,commission_rate,commission_amount_local,commission_amount_millicents,newrevenue_jobsearch_millicents,newrevenue_dradis_lifetime_millicents,newrevenue_dradis_recurring_millicents,newrevenue_resume_millicents,currency,date_modified)
                  |VALUES (
                  | ${if (value.date == null) null else "'" + value.date.replaceAll("'", "''")+ "'"}
                  |,${value.advertiser_id.getOrElse(null)}
                  |,${value.salesrep_id.getOrElse(null)}
                  |,${value.revenue_jobsearch_millicents.getOrElse(null)}
                  |,${value.revenue_dradis_lifetime_millicents.getOrElse(null)}
                  |,${value.revenue_dradis_recurring_millicents.getOrElse(null)}
                  |,${value.revenue_resume_millicents.getOrElse(null)}
                  |,${value.revenue_ineligible_millicents.getOrElse(null)}
                  |,${value.revenue_jobsearch_local.getOrElse(null)}
                  |,${value.revenue_dradis_lifetime_local.getOrElse(null)}
                  |,${value.revenue_dradis_recurring_local.getOrElse(null)}
                  |,${value.revenue_resume_local.getOrElse(null)}
                  |,${value.revenue_ineligible_local.getOrElse(null)}
                  |,${value.discount_local.getOrElse(null)}
                  |,${value.discount_forward_local.getOrElse(null)}
                  |,${value.discounted_revenue_local.getOrElse(null)}
                  |,${value.commission_rate.getOrElse(null)}
                  |,${value.commission_amount_local.getOrElse(null)}
                  |,${value.commission_amount_millicents.getOrElse(null)}
                  |,${value.newrevenue_jobsearch_millicents.getOrElse(null)}
                  |,${value.newrevenue_dradis_lifetime_millicents.getOrElse(null)}
                  |,${value.newrevenue_dradis_recurring_millicents.getOrElse(null)}
                  |,${value.newrevenue_resume_millicents.getOrElse(null)}
                  |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''")+ "'"}
                  |,${if (value.date_modified == null) null else "'" + value.date_modified.replaceAll("'", "''")+ "'"}
                  |)
                """.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
  }
  def close(errorOrNull: Throwable): Unit = {}
}


val tblADCaccounts_salesrep_commissionsQuery = tblADCaccounts_salesrep_commissions.as[TblADCaccounts_salesrep_commissions].writeStream.foreach(tblADCaccounts_salesrep_commissionsWriter).outputMode("append").start



case class TblADCadvertiser_rep_revenues(
 opType: String
,activity_date: java.sql.Date
,advertiser_id: Option[BigInt]
,relationship: String
,user_id: Option[BigInt]
,revenue: Option[BigDecimal]
,revenue_jobsearch_millicents: Option[BigInt]
,revenue_resume_millicents: Option[BigInt]
,revenue_jobsearch_local: Option[BigInt]
,revenue_resume_local: Option[BigInt]
,currency: String
,revenue_ss_millicents: Option[BigDecimal]
,revenue_hj_millicents: Option[BigDecimal]
,revenue_hjr_millicents: Option[BigDecimal]
,revenue_ss_local: Option[BigInt]
,revenue_hj_local: Option[BigInt]
,revenue_hjr_local: Option[BigInt]
,date_modified: Timestamp
)


val tblADCadvertiser_rep_revenuesSchema = StructType(Array(
          StructField("database", StringType),
          StructField("table", StringType),
          StructField("type", StringType),
          StructField("ts", TimestampType),
          StructField("position", StringType),
          StructField("primary_key", StructType(Array(
                         StructField("activity_date", DateType)
                        ,StructField("advertiser_id", LongType)
                        ,StructField("relationship", StringType)))),
          StructField("data", StructType(Array(
                         StructField("activity_date", DateType)
                        ,StructField("advertiser_id", LongType)
                        ,StructField("relationship", StringType)
                        ,StructField("user_id", LongType)
                        ,StructField("revenue", DecimalType(7,2))
                        ,StructField("revenue_jobsearch_millicents", LongType)
                        ,StructField("revenue_resume_millicents", LongType)
                        ,StructField("revenue_jobsearch_local", LongType)
                        ,StructField("revenue_resume_local", LongType)
                        ,StructField("currency", StringType)
                        ,StructField("revenue_ss_millicents", DecimalType(20,0))
                        ,StructField("revenue_hj_millicents", DecimalType(20,0))
                        ,StructField("revenue_hjr_millicents", DecimalType(20,0))
                        ,StructField("revenue_ss_local", LongType)
                        ,StructField("revenue_hj_local", LongType)
                        ,StructField("revenue_hjr_local", LongType)
                        ,StructField("date_modified", TimestampType)))),
          StructField("old", StructType(Array(
                         StructField("activity_date", DateType)
                        ,StructField("advertiser_id", LongType)
                        ,StructField("relationship", StringType)
                        ,StructField("user_id", LongType)
                        ,StructField("revenue", DecimalType(7,2))
                        ,StructField("revenue_jobsearch_millicents", LongType)
                        ,StructField("revenue_resume_millicents", LongType)
                        ,StructField("revenue_jobsearch_local", LongType)
                        ,StructField("revenue_resume_local", LongType)
                        ,StructField("currency", StringType)
                        ,StructField("revenue_ss_millicents", DecimalType(20,0))
                        ,StructField("revenue_hj_millicents", DecimalType(20,0))
                        ,StructField("revenue_hjr_millicents", DecimalType(20,0))
                        ,StructField("revenue_ss_local", LongType)
                        ,StructField("revenue_hj_local", LongType)
                        ,StructField("revenue_hjr_local", LongType)
                        ,StructField("date_modified", TimestampType))))
          ))

val tblADCadvertiser_rep_revenues = rawData.select(from_json($"value", tblADCadvertiser_rep_revenuesSchema).as("value")).filter($"value.table" === "tblADCadvertiser_rep_revenues").select($"value.type".as("opType"), $"value.data.*")

tblADCadvertiser_rep_revenues.createOrReplaceTempView("tblADCadvertiser_rep_revenues")


val tblADCadvertiser_rep_revenuesWriter = new ForeachWriter[TblADCadvertiser_rep_revenues] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: TblADCadvertiser_rep_revenues): Unit = {

    def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

    if (value.opType == "insert" || value.opType == "update")
      {
        val cQuery1 =
                s"""

                |INSERT INTO adcentraldb.tblADCadvertiser_rep_revenues (activity_date,advertiser_id,relationship,user_id,revenue,revenue_jobsearch_millicents,revenue_resume_millicents,revenue_jobsearch_local,revenue_resume_local,currency,revenue_ss_millicents,revenue_hj_millicents,revenue_hjr_millicents,revenue_ss_local,revenue_hj_local,revenue_hjr_local,date_modified)
                |VALUES (
                | ${if (value.activity_date == null) null else "'" + value.activity_date+ "'"}
                |,${value.advertiser_id.getOrElse(null)}
                |,${if (value.relationship == null) null else "'" + value.relationship.replaceAll("'", "''")+ "'"}
                |,${value.user_id.getOrElse(null)}
                |,${value.revenue.getOrElse(null)}
                |,${value.revenue_jobsearch_millicents.getOrElse(null)}
                |,${value.revenue_resume_millicents.getOrElse(null)}
                |,${value.revenue_jobsearch_local.getOrElse(null)}
                |,${value.revenue_resume_local.getOrElse(null)}
                |,${if (value.currency == null) null else "'" + value.currency.replaceAll("'", "''")+ "'"}
                |,${value.revenue_ss_millicents.getOrElse(null)}
                |,${value.revenue_hj_millicents.getOrElse(null)}
                |,${value.revenue_hjr_millicents.getOrElse(null)}
                |,${value.revenue_ss_local.getOrElse(null)}
                |,${value.revenue_hj_local.getOrElse(null)}
                |,${value.revenue_hjr_local.getOrElse(null)}
                |,${if (value.date_modified == null) null else "'" + value.date_modified+ "'"}
                |)
                """.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
  }
  def close(errorOrNull: Throwable): Unit = {}
}

val tblADCadvertiser_rep_revenuesQuery = tblADCadvertiser_rep_revenues.as[TblADCadvertiser_rep_revenues].writeStream.foreach(tblADCadvertiser_rep_revenuesWriter).outputMode("append").start
val tblADCadvertiser_rep_revenuesConsoleQuery = tblADCadvertiser_rep_revenues.as[TblADCadvertiser_rep_revenues].writeStream.outputMode("append").format("console").start




case class TblADCparent_company_advertisers(
 opType: String
,parent_company_id: BigInt
,advertiser_id: BigInt
,date_created: Timestamp
,assignment_method: String
,assigned_by: Option[BigInt]
,date_modified: Timestamp
)

val tblADCparent_company_advertisersSchema = StructType(Array(
          StructField("database", StringType),
          StructField("table", StringType),
          StructField("type", StringType),
          StructField("ts", TimestampType),
          StructField("position", StringType),
          StructField("primary_key", StructType(Array(
             StructField("parent_company_id", LongType)
            ,StructField("advertiser_id", LongType)))),
          StructField("data", StructType(Array(
             StructField("parent_company_id", LongType)
            ,StructField("advertiser_id", LongType)
            ,StructField("date_created", TimestampType)
            ,StructField("assignment_method", StringType)
            ,StructField("assigned_by", LongType)
            ,StructField("date_modified", TimestampType)))),
          StructField("old", StructType(Array(
             StructField("parent_company_id", LongType)
            ,StructField("advertiser_id", LongType)
            ,StructField("date_created", TimestampType)
            ,StructField("assignment_method", StringType)
            ,StructField("assigned_by", LongType)
            ,StructField("date_modified", TimestampType))))
          ))

val tblADCparent_company_advertisersWriter = new ForeachWriter[TblADCparent_company_advertisers] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: TblADCparent_company_advertisers): Unit = {

    def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

    if (value.opType == "insert" || value.opType == "update")
      {
        val cQuery1 =
                s"""
                    |INSERT INTO adcentraldb.tblADCparent_company_advertisers (parent_company_id,advertiser_id,date_created,assignment_method,assigned_by,date_modified)
                    |VALUES (
                    | ${value.parent_company_id}
                    |,${value.advertiser_id}
                    |,${if (value.date_created == null) null else "'" + value.date_created+ "'"}
                    |,${if (value.assignment_method == null) null else "'" + value.assignment_method.replaceAll("'", "''")+ "'"}
                    |,${value.assigned_by.getOrElse(null)}
                    |,${if (value.date_modified == null) null else "'" + value.date_modified+ "'"}
                    |)
                """.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
    else if (value.opType == "delete")
      {
        val cQuery1 =
                s"""
                    |DELETE FROM adcentraldb.tblADCparent_company_advertisers
                    |WHERE parent_company_id = ${value.parent_company_id}
                    |AND advertiser_id = ${value.advertiser_id}
                """.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
  }
  def close(errorOrNull: Throwable): Unit = {}
}

// val liveCountWriter = new ForeachWriter[LiveClickCount] {

//       def open(partitionId: Long, version: Long): Boolean = true

//       def process(value: LiveClickCount): Unit = {
//         val cQuery1 = s"UPDATE $keyspace.click_count_by_interval SET click_count = click_count + ${value.click_count} WHERE time = '${value.timestamp}'"
//         connector.withSessionDo(session => session.execute(cQuery1))
//       }

//       def close(errorOrNull: Throwable): Unit = {}
//     }

// val windowedCounts = clickRawDataDS.toDF.withWatermark("timestamp", "10 minutes").groupBy(window($"timestamp", "1 second", "1 second")).count()

// val liveCountQuery = windowedCounts.selectExpr("window.end AS timestamp", "CAST(count AS Int) AS click_count").as[LiveClickCount].writeStream.outputMode("update").foreach(liveCountWriter).start

// liveCountQuery.awaitTermination()

// Write a program to auto generate datasta.
// n = Pick a random no from 1 to 50
// for i in 1 to n generate current timestamp, random country, random city and push these records to kafka
// Click Count Every Second - select SUM(clicks) AS clicks from click_count WHERE time > '2018-01-02 06:00:00' AND time < '2018-01-03 06:00:02' ALLOW Filtering;




// val query = ds2.writeStream.outputMode("append").format("console").start

// val offsetQuery = df.select("offset").writeStream.outputMode("append").format("console").start

// val sparkConf = new SparkConf().setAppName("DirectKafkaClickstreams")

// Create context with 10-second batch intervals
// val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

// Create direct Kafka stream with brokers and topics
// val topicsSet = topics.split(",").toSet

// val kafkaParams = Map[String, Object](
//   "bootstrap.servers" -> brokers,
//   "key.deserializer" -> classOf[StringDeserializer],
//   "value.deserializer" -> classOf[StringDeserializer],
//   "group.id" -> "1",
//   "auto.offset.reset" -> "latest",
//   "enable.auto.commit" -> (false: java.lang.Boolean)
// )

// val stream = KafkaUtils.createDirectStream[String, String](
//   ssc,
//   PreferConsistent,
//   Subscribe[String, String](topicsSet, kafkaParams)
// )

// Drop the table if it already exists 
// spark.sql("DROP TABLE IF EXISTS test")

// // Create the table to store your streams 
// spark.sql("CREATE TABLE test ( message string) STORED AS TEXTFILE")

// /** Case class for converting RDD to DataFrame */
// case class Record(message: String)

// var offsetRanges = Array[OffsetRange]()
// // Convert RDDs of the lines DStream to DataFrame and run a SQL query
// stream.transform { 
//     rdd => 
//       offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//       rdd
//   }.map(_.value).foreachRDD { (rdd: RDD[String], time: Time) =>
 
//  import spark.implicits._
//   // Convert RDD[String] to RDD[case class] to DataFrame

//  val messagesDataFrame = rdd.map(w => Record(w)).toDF()
  
//   // Creates a temporary view using the DataFrame
//   messagesDataFrame.createOrReplaceTempView("csmessages")
  
//   //Insert continuous streams into Hive table
//   // spark.sql("INSERT INTO TABLE test SELECT * FROM csmessages")

//   // Select the parsed messages from the table using SQL and print it (since it runs on drive display few records)
//   val messagesqueryDataFrame =
//   spark.sql("SELECT * FROM csmessages")
//   println(s"========= $time =========")
//   messagesqueryDataFrame.show()

//   stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
// }

// // Start the computation
// ssc.start()
// ssc.awaitTerminationOrTimeout(10000)
// ssc.stop(false, false)










// Reading JSON Alternative.
// val mySchema = StructType(Array(
//  StructField("database", StringType),
//  StructField("table", StringType),
//  StructField("type", StringType),
//  StructField("ts", TimestampType),
//  StructField("position", StringType),
//  StructField("primary_key", StructType(Array(StructField("date", DateType), StructField("advertiser_id", IntegerType)))),
//  StructField("data", StructType(Array(
//                  StructField("date", DateType),
//                  StructField("advertiser_id", IntegerType),
//                  StructField("salesrep_id", IntegerType),
//                  StructField("revenue_jobsearch_millicents", LongType),
//                  StructField("revenue_dradis_lifetime_millicents", LongType),
//                  StructField("revenue_dradis_recurring_millicents", LongType),
//                  StructField("revenue_resume_millicents", LongType),
//                  StructField("revenue_ineligible_millicents", LongType),
//                  StructField("revenue_jobsearch_local", IntegerType),
//                  StructField("revenue_dradis_lifetime_local", IntegerType),
//                  StructField("revenue_dradis_recurring_local", IntegerType),
//                  StructField("revenue_resume_local", IntegerType),
//                  StructField("revenue_ineligible_local", IntegerType),
//                  StructField("discount_local", IntegerType),
//                  StructField("discount_forward_local", IntegerType),
//                  StructField("discounted_revenue_local", IntegerType),
//                  StructField("commission_rate", DoubleType),
//                  StructField("commission_amount_local", IntegerType),
//                  StructField("commission_amount_millicents", LongType),
//                  StructField("newrevenue_jobsearch_millicents", LongType),
//                  StructField("newrevenue_dradis_lifetime_millicents", LongType),
//                  StructField("newrevenue_dradis_recurring_millicents", LongType),
//                  StructField("newrevenue_resume_millicents", LongType),
//                  StructField("currency", StringType),
//                  StructField("date_modified", TimestampType)
//           ))),
//  StructField("old", StructType(Array(
//                  StructField("date", DateType),
//                  StructField("advertiser_id", IntegerType),
//                  StructField("salesrep_id", IntegerType),
//                  StructField("revenue_jobsearch_millicents", LongType),
//                  StructField("revenue_dradis_lifetime_millicents", LongType),
//                  StructField("revenue_dradis_recurring_millicents", LongType),
//                  StructField("revenue_resume_millicents", LongType),
//                  StructField("revenue_ineligible_millicents", LongType),
//                  StructField("revenue_jobsearch_local", IntegerType),
//                  StructField("revenue_dradis_lifetime_local", IntegerType),
//                  StructField("revenue_dradis_recurring_local", IntegerType),
//                  StructField("revenue_resume_local", IntegerType),
//                  StructField("revenue_ineligible_local", IntegerType),
//                  StructField("discount_local", IntegerType),
//                  StructField("discount_forward_local", IntegerType),
//                  StructField("discounted_revenue_local", IntegerType),
//                  StructField("commission_rate", DoubleType),
//                  StructField("commission_amount_local", IntegerType),
//                  StructField("commission_amount_millicents", LongType),
//                  StructField("newrevenue_jobsearch_millicents", LongType),
//                  StructField("newrevenue_dradis_lifetime_millicents", LongType),
//                  StructField("newrevenue_dradis_recurring_millicents", LongType),
//                  StructField("newrevenue_resume_millicents", LongType),
//                  StructField("currency", StringType),
//                  StructField("date_modified", TimestampType)
//           )))
// ))

// val df1 = a.select(from_json($"value", mySchema).as("value")).select("value.data.*")


// val a = Seq("""{
//     "database": "adcentraldb",
//     "table": "tblADCaccounts_salesrep_commissions",
//     "type": "update",
//     "ts": 1533328520,
//     "position": "aus-dbs25-D-bin.033814:602268953",
//     "primary_key": {
//         "date": "2018-08-03",
//         "advertiser_id": 21338433
//     },
//     "data": {
//         "date": "2018-08-03",
//         "advertiser_id": 21338433,
//         "salesrep_id": 10714,
//         "revenue_jobsearch_millicents": 2449613,
//         "revenue_dradis_lifetime_millicents": 0,
//         "revenue_dradis_recurring_millicents": 2488962,
//         "revenue_resume_millicents": 0,
//         "revenue_ineligible_millicents": 0,
//         "revenue_jobsearch_local": 1867,
//         "revenue_dradis_lifetime_local": 0,
//         "revenue_dradis_recurring_local": 1897,
//         "revenue_resume_local": 0,
//         "revenue_ineligible_local": 0,
//         "discount_local": 0,
//         "discount_forward_local": 0,
//         "discounted_revenue_local": 3764,
//         "commission_rate": 0.150,
//         "commission_amount_local": 565,
//         "commission_amount_millicents": 741308,
//         "newrevenue_jobsearch_millicents": 0,
//         "newrevenue_dradis_lifetime_millicents": 0,
//         "newrevenue_dradis_recurring_millicents": 0,
//         "newrevenue_resume_millicents": 0,
//         "currency": "GBP",
//         "date_modified": "2018-08-03 20:35:20"
//     },
//     "old": {
//         "revenue_jobsearch_millicents": 2132095,
//         "revenue_jobsearch_local": 1625,
//         "discounted_revenue_local": 3522,
//         "commission_amount_local": 528,
//         "commission_amount_millicents": 692762,
//         "date_modified": "2018-08-03 20:04:40"
//     }
// }""").toDS