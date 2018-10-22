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
import org.apache.spark.sql.streaming.Trigger


val Array(brokers, topics) = Array("ec2-54-160-85-68.compute-1.amazonaws.com:9092", "maxwell")

val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topics).load()

val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

val tables = Set("tbladvertiser", "tblADScurrency_rates", "tblADCaccounts_salesrep_commissions", "tblADCadvertiser_rep_revenues", "tblADCparent_company_advertisers", "tblADCparent_company", "tblCRMgeneric_product_credit", "tblADCquota", "tblACLusers", "tblADCsummary_advertiser")

val postgresql = new PostgreSql("...", "aguyyala", "...")

def getDatasetId(tbl: String) = {
    val query = s"select dataset_id from eravana.dataset where name='$tbl'"

    val rs = postgresql.executeQuery(query)

    var dataset_id = -1
    while (rs.next()) dataset_id = rs.getInt("dataset_id")

    dataset_id
}


def getColumns(dataset_id: Int) = {
    val query = s"select name, data_type from eravana.dataset_column where dataset_id = $dataset_id order by ordinal_position"

    val rs = postgresql.executeQuery(query)

    val result = mutable.ArrayBuffer[(String, String)]()

    while (rs.next()) result += ((rs.getString("name"), rs.getString("data_type")))

    result.toArray
}



def buildMetadata(tables: Set[String]) = {
    val result = mutable.Map[String, Array[(String, String)]]()
    
    tables.foreach{tbl =>
        val dataset_id = getDatasetId(tbl)
        val columns = getColumns(dataset_id)
        result += tbl -> columns
    }

    result.toMap
}


def postgresqlToSparkDataType(dataType: String): DataType = {

  dataType match {
    case "DATETIME" => TimestampType
    case "TIME" => StringType
    case "DATE" => DateType
    case "SMALLINT" => IntegerType
    case "TIMESTAMP" => TimestampType
    case "FLOAT" => DoubleType
    case "INTEGER" => IntegerType
    case "VARCHAR" => StringType
    case "NUMERIC" => DoubleType
    case "BIGINT" => LongType
    case "UUID" => StringType
    case "BOOLEAN" => IntegerType
    case "DOUBLE" => DoubleType
    case "YEAR" => IntegerType
    case _ => StringType
  }
}


val messageSchema = StructType(Array(
 StructField("database", StringType),
 StructField("table", StringType),
 StructField("type", StringType),
 StructField("ts", TimestampType),
 StructField("position", StringType),
 StructField("primary_key", StringType),
 StructField("data", StringType),
 StructField("old", StringType)
))

val res = buildMetadata(tables)

tables.foreach{tbl =>
    val js = StructType(res(tbl).map(c => StructField(c._1, postgresqlToSparkDataType(c._2))).toArray)
    val df = rawData.select(from_json($"value", messageSchema).as("value")).filter($"value.table" === tbl).select($"value.type".as("opType"), from_json($"value.data", js).as("data")).select($"opType", $"data.*").where("opType IN ('insert', 'update', 'delete')")

    df.withColumn("dt", current_date).withColumn("hr", hour(current_timestamp)).writeStream.format("parquet").option("checkpointLocation", s"s3a://indeed-data/datalake/v1/stage/binlog_events/checkpoint/$tbl/").option("path", s"s3a://indeed-data/datalake/v1/stage/binlog_events/$tbl/").trigger(Trigger.ProcessingTime("5 minutes")).partitionBy("dt", "hr").start()
}

// val tables = Set("tbladvertiser", "tblADCaccounts_salesrep_commissions")
// var tbl = "tbladvertiser"
// var js = StructType(res(tbl).map(c => StructField(c._1, postgresqlToSparkDataType(c._2))).toArray)

// val tbladvertiser = rawData.select(from_json($"value", messageSchema).as("value")).filter($"value.table" === tbl).select($"value.type".as("opType"), from_json($"value.data", js).as("data")).select($"opType", $"data.*").where("opType IN ('insert', 'update', 'delete')")
// val q = tbladvertiser.writeStream.format("console").start
// val tbladvertiserQuery = tbladvertiser.withColumn("dt", current_date).withColumn("hr", hour(current_timestamp)).writeStream.format("parquet").option("checkpointLocation", s"s3a://indeed-data/datalake/v1/stage/binlog_events/checkpoint/$tbl/").option("path", s"s3a://indeed-data/datalake/v1/stage/binlog_events/$tbl/").trigger(Trigger.ProcessingTime("5 minutes")).partitionBy("dt", "hr").start()


// tbl = "tblADCaccounts_salesrep_commissions"
// js = StructType(res(tbl).map(c => StructField(c._1, postgresqlToSparkDataType(c._2))).toArray)

// val tblADCaccounts_salesrep_commissions = rawData.select(from_json($"value", messageSchema).as("value")).filter($"value.table" === tbl).select(from_json($"value.data", js).as("data")).select($"data.*")
// val tblADCaccounts_salesrep_commissionsQuery = tblADCaccounts_salesrep_commissions.writeStream.format("parquet").option("checkpointLocation", s"s3a://indeed-data/datalake/v1/stage/binlog_events/checkpoint/$tbl/").option("path", s"s3a://indeed-data/datalake/v1/stage/binlog_events/$tbl/").trigger(Trigger.ProcessingTime("5 minutes")).start()

// --metadata.url=... --metadata.user=aguyyala --metadata.password=... --whitelistedTables=tbladvertiser,tblADScurrency_rates,tblADCaccounts_salesrep_commissions,tblADCadvertiser_rep_revenues,tblADCparent_company_advertisers,tblADCparent_company,tblCRMgeneric_product_credit,tblADCquota,tblACLusers   --targetFormat=parquet  --baseLoc=s3a://indeed-data/datalake/v1/stage/binlog_events --runInterval=5



// val tbladvertiser = rawData.select(from_json($"value", messageSchema).as("value")).filter($"value.table" === tbl).select($"value.type".as("opType"), from_json($"value.data", tblAdvertiserSchema).as("data")).select($"opType", $"data.*").where("opType IN ('insert', 'update', 'delete')")
// val q = tbladvertiser.writeStream.format("console").start

// q.stop

// val a = Seq("""{"id":23338863,"account_id":382334568,"company":"Gran Alianza","contact":"Liliana Flores ","url":"","address1":"","address2":"","city":"","state":"","zip":"","phone":"5525600303","phone_type":"UNKNOWN","verified_phone":null,"verified_phone_extension":null,"uuidstring":"034926c4-ae9c-45ee-8989-bc929db05c71","date_created":"2018-09-13 09:27:06","active":2,"ip_address":"189.146.195.12","referral_id":241951,"monthly_budget":-1.00,"expended_budget":0.00,"type":"Direct Employer","advertiser_number":"9232424478201459","show_conversions":0,"billing_threshold":500.00,"payment_method":"POSTPAY_CC","industry":null,"agency_discount":0.000,"is_ad_agency":0,"process_level":"none","estimated_budget":0.00,"first_revenue_date":null,"last_revenue_date":null,"first_revenue_override":null,"terms":"Due upon receipt","currency":"USD","monthly_budget_local":-1,"expended_budget_local":0,"billing_threshold_local":null,"estimated_budget_local":0,"employee_count":"50-149","last_updated":"2018-10-22 10:56:16"}""").toDS


// val tblAdvertiserSchema = StructType(Array(
//                     StructField("id",  LongType),
//                     StructField("account_id",  LongType),
//                     StructField("company",  StringType),
//                     StructField("contact",  StringType),
//                     StructField("url",  StringType),
//                     StructField("address1",  StringType),
//                     StructField("address2",  StringType),
//                     StructField("city",  StringType),
//                     StructField("state",  StringType),
//                     StructField("zip",  StringType),
//                     StructField("phone",  StringType),
//                     StructField("phone_type",  StringType),
//                     StructField("verified_phone",  StringType),
//                     StructField("verified_phone_extension",  StringType),
//                     StructField("uuidstring",  StringType),
//                     StructField("date_created",  TimestampType),
//                     StructField("active",  IntegerType),
//                     StructField("ip_address",  StringType),
//                     StructField("referral_id",  LongType),
//                     StructField("monthly_budget",  DoubleType),
//                     StructField("expended_budget",  DoubleType),
//                     StructField("type",  StringType),
//                     StructField("advertiser_number",  StringType),
//                     StructField("show_conversions",  IntegerType),
//                     StructField("billing_threshold",  DoubleType),
//                     StructField("payment_method",  StringType),
//                     StructField("industry",  StringType),
//                     StructField("agency_discount",  DoubleType),
//                     StructField("is_ad_agency",  IntegerType),
//                     StructField("process_level",  StringType),
//                     StructField("estimated_budget",  DoubleType),
//                     StructField("first_revenue_date",  DateType),
//                     StructField("last_revenue_date",  DateType),
//                     StructField("first_revenue_override",  DateType),
//                     StructField("terms",  StringType),
//                     StructField("currency",  StringType),
//                     StructField("monthly_budget_local",  LongType),
//                     StructField("expended_budget_local",  LongType),
//                     StructField("billing_threshold_local",  LongType),
//                     StructField("estimated_budget_local",  LongType),
//                     StructField("employee_count",  StringType),
//                     StructField("last_updated",  TimestampType)
//                 ))

// a.select(from_json($"value", tblAdvertiserSchema).as("data")).select($"data.*").show(false)
// a.select(from_json($"value", tblAdvertiserSchema).as("data")).select($"data.*").select("date_created", "referral_id", "last_updated", "first_revenue_date", "last_revenue_date", "first_revenue_override").show(false)

// a.select(from_json($"value", js).as("data")).select($"data.*").show(false)


val tbl = "tblADCparent_company"
val a = Seq("""{"id":1038797,"company":"Step II Inc","sales_region":"","user_id":0,"first_revenue_date":"2016-06-01","default_advertiser_id":0,"prospecting_status":"UNAVAILABLE","hq_city":"Omaha","hq_state":"NE","hq_zip":"68130-4614","hq_country":"US","duns":"033229449","location_type":"PRIMARY_ADDRESS","revenue":340000,"total_employees":8,"franchise_operation_type":"","is_subsidiary":0,"doing_business_as":"Edgewood Vista Senior Living","exchange_symbol":"","exchange":"","USSIC":"80510000","USSIC_description":"Skilled nursing care facilities","NAICS":"623110","NAICS_description":"Nursing Care Facilities","parent_name":"","parent_duns":"","ultimate_domestic_parent_name":"","ultimate_domestic_parent_duns":"","ultimate_parent_name":"","ultimate_parent_duns":"","active_jobs":219,"date_created":"2015-05-04 23:12:37","is_lead_eligible":0,"lead_score":null,"date_modified":"2018-10-22 18:15:50"}""").toDS
val js = StructType(res(tbl).map(c => StructField(c._1, postgresqlToSparkDataType(c._2))))
a.select(from_json($"value", js).as("data")).select($"data.*").show(false)

