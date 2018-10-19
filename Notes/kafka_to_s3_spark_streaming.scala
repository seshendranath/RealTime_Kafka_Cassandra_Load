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

val tables = Set("tbladvertiser", "tblADCaccounts_salesrep_commissions")

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
    case "TIME" => TimestampType
    case "DATE" => DateType
    case "SMALLINT" => IntegerType
    case "TIMESTAMP" => TimestampType
    case "FLOAT" => FloatType
    case "INTEGER" => IntegerType
    case "VARCHAR" => StringType
    case "NUMERIC" => IntegerType
    case "BIGINT" => LongType
    case "UUID" => StringType
    case "BOOLEAN" => BooleanType
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
 StructField("old", StringType),
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
// val tbladvertiserQuery = tbladvertiser.withColumn("dt", current_date).withColumn("hr", hour(current_timestamp)).writeStream.format("parquet").option("checkpointLocation", s"s3a://indeed-data/datalake/v1/stage/binlog_events/checkpoint/$tbl/").option("path", s"s3a://indeed-data/datalake/v1/stage/binlog_events/$tbl/").trigger(Trigger.ProcessingTime("5 minutes")).partitionBy("dt", "hr").start()


// tbl = "tblADCaccounts_salesrep_commissions"
// js = StructType(res(tbl).map(c => StructField(c._1, postgresqlToSparkDataType(c._2))).toArray)

// val tblADCaccounts_salesrep_commissions = rawData.select(from_json($"value", messageSchema).as("value")).filter($"value.table" === tbl).select(from_json($"value.data", js).as("data")).select($"data.*")
// val tblADCaccounts_salesrep_commissionsQuery = tblADCaccounts_salesrep_commissions.writeStream.format("parquet").option("checkpointLocation", s"s3a://indeed-data/datalake/v1/stage/binlog_events/checkpoint/$tbl/").option("path", s"s3a://indeed-data/datalake/v1/stage/binlog_events/$tbl/").trigger(Trigger.ProcessingTime("5 minutes")).start()

// --metadata.url=... --metadata.user=aguyyala --metadata.password=... --whitelistedTables=tbladvertiser,tblADScurrency_rates,tblADCaccounts_salesrep_commissions,tblADCadvertiser_rep_revenues,tblADCparent_company_advertisers,tblADCparent_company,tblCRMgeneric_product_credit,tblADCquota,tblACLusers   --targetFormat=parquet  --baseLoc=s3a://indeed-data/datalake/v1/stage/binlog_events --runInterval=5


