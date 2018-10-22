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
import java.sql.Date

spark.conf.set("spark.sql.shuffle.partitions", "10")
spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
val Array(brokers, topics) = Array("ec2-54-85-62-208.compute-1.amazonaws.com:9092", "maxwell")

val timestampFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127"))

val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topics).load()

val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)

val quoteTypes = Set("StringType", "DateType", "TimestampType")

def sparktoScalaDataType(dataType: String): String = {
  val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

  dataType match {
    case "StringType" => "String"
    case "LongType" => "BigInt"
    case "DoubleType" => "Double"
    case "IntegerType" => "Int"
    case "DateType" => "Date"
    case "TimestampType" => "Timestamp"
    case "BooleanType" => "Int"
    case decimalPattern() => "BigDecimal" //"Double"// dataType.replace("Type", "").toLowerCase
    case _ => "String"
  }
}

def convertBoolToInt(dtype: String) = if (dtype == "BooleanType") "IntegerType" else dtype

def getScalaType(pk:Set[String], col: String, dtype: String): String = if ((quoteTypes contains dtype) || (pk contains col)) sparktoScalaDataType(dtype) else "Option[" + sparktoScalaDataType(dtype) + "]"

val db = "adcentraldb"
val table = "tblADCaccounts_salesrep_commissions"
val pk = Set("date", "advertiser_id")

val df = spark.read.parquet(s"s3a://indeed-data/datalake/v1/prod/mysql/$db/$table")
val cols = df.columns.mkString(",")

// Produce Case Statement
val caseStmt = "case class " + table.capitalize + "(\n opType: String\n" + df.dtypes.map{case (col, dtype) => "," + col + ": " + getScalaType(pk, col, dtype)}.mkString("\n") + "\n)"

// Generate PK Statement
val pkSchema = "\n\t\t\t\t\t\t " + df.dtypes.filter{ case (col, _) => pk contains col}.map{case (col, dtype) => s"""StructField("$col", ${convertBoolToInt(dtype)})"""}.mkString("\n\t\t\t\t\t\t,")

// Generate Data Schema
val tableSchema = "\n\t\t\t\t\t\t " + df.dtypes.map{case (col, dtype) => s"""StructField("$col", ${convertBoolToInt(dtype)})"""}.mkString("\n\t\t\t\t\t\t,")

// Generates JSON Schema
val jSchema = s"""
 |object TblADCaccounts_salesrep_commissions {
 |  val jsonSchema = StructType(Array(
 |          StructField("database", StringType),
 |          StructField("table", StringType),
 |          StructField("type", StringType),
 |          StructField("ts", TimestampType),
 |          StructField("position", StringType),
 |          StructField("primary_key", StructType(Array(${pkSchema}))),
 |          StructField("data", StructType(Array(${tableSchema}))),
 |          StructField("old", StructType(Array(${tableSchema})))
 |          ))
 |}
 """.stripMargin

// Generates val stmt
val valStmt = s"""val $table = rawData.select(from_json($$"value", ${table.capitalize}.jsonSchema).as("value")).filter($$"value.table" === "$table").select($$"value.type".as("opType"), $$"value.data.*")"""

// val q = tblAdvertiser.as[TblAdvertiser].writeStream.outputMode("append").format("console").start
// q.stop


// Generates insert values stmt
val insertValues = s"|INSERT INTO ${db}.${table} ($cols)\n|VALUES (" + "\n| " + df.dtypes.map{case (col, dtype) => if (pk contains col) { if (quoteTypes contains dtype) s"""'$${value.$col}'""" else s"""$${value.$col}""" } else if (dtype == "StringType") s"""$${if (value.$col == null) null else "'" + value.$col.replaceAll("'", "''")+ "'"}""" else if (quoteTypes contains dtype) s"""$${if (value.$col == null) null else "'" + value.$col+ "'"}""" else s"""$${value.$col.orNull}"""}.mkString("\n|,") + "\n|)"

val deleteValues = s"|DELETE FROM ${db}.${table}\n|WHERE " + pk.map(c => s"$c = $${value.$c}").mkString("\n|AND ") 

// Generates ForeachWriter
val writer = s"""
val ${table}Writer = new ForeachWriter[${table.capitalize}] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: ${table.capitalize}): Unit = {

    def intBool(i: Any): Any = if (i==null) null else if (i==0) false else true

    if (value.opType == "insert" || value.opType == "update")
      {
        val cQuery1 =
                s${'"'}${'"'}${'"'}
                ${insertValues}
                ${'"'}${'"'}${'"'}.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
    else if (value.opType == "delete")
      {
        val cQuery1 =
                s${'"'}${'"'}${'"'}
                ${deleteValues}
                ${'"'}${'"'}${'"'}.stripMargin
        connector.withSessionDo{session =>session.execute(cQuery1)}
      }
  }
  def close(errorOrNull: Throwable): Unit = {}
}"""


val streamStmt = s"""val ${table}Query = $table.as[${table.capitalize}].writeStream.foreach(${table}Writer).outputMode("append").start"""


println(caseStmt)
println(jSchema)

println(valStmt)
println(writer)
println(streamStmt)
