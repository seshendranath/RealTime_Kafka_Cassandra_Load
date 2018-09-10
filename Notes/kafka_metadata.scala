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
import com.datastax.spark.connector.streaming._
import collection.JavaConverters._
import scala.collection.mutable

val cluster = Cluster.builder().addContactPoints(c.split(","): _*).build()
val session = cluster.connect("metadata")

val query = s"select topic, partition, offset from streaming_metadata where job = 'SalesSummary_Load';"
val res = session.execute(query).all.asScala.toArray

val resMap = mutable.Map[String, mutable.Map[Int, Long]]()

res.foreach {rec =>
    val topic = rec.getString("topic")
    val partition = rec.getInt("partition")
    val offset = rec.getLong("offset")
    val value = resMap.getOrElse(topic, mutable.Map[Int, Long]())
    resMap += topic -> (value + (partition -> Math.min(value.getOrElse(partition, offset), offset)))
}

val assignString = "{" + resMap.map{ case (k, v) => s""""$k":[${v.map(_._1).mkString(",")}]"""}.mkString(",") + "}"
val offsetString = "{" + resMap.map{ case (k, v) => s""""$k":{${v.map{case (p, o) => '"' + s"$p" + '"' + s":$o"}.mkString(",")}}"""}.mkString(",") + "}"


val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("assign", assignString).option("startingOffsets", offsetString).load()

val q = kafkaStream.writeStream.format("console").start()

q.stop


spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.cassandra.input.consistency.level", "LOCAL_ONE")
spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_ONE")

spark.conf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127")

val Array(brokers, topics) = Array("ec2-54-160-85-68.compute-1.amazonaws.com:9092,ec2-54-226-72-146.compute-1.amazonaws.com:9092", "maxwell")

val timestampFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")

val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127"))

val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", topics).option("failOnDataLoss", "false").load()

/* 
 db text, 
 tbl text, 
 tbl_date_modified timestamp, 
 topic text,
 partition int,
 offset bigint,
 primary_key text,
 binlog_position text,
 kafka_timestamp timestamp, 
 binlog_timestamp timestamp, 
 last_updated timestamp,
*/

object KafkaMetadata {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StringType),
    StructField("data", StructType(Array(
        StructField("last_updated", TimestampType)
      , StructField("date_modified", TimestampType))))
    ))
}

val rawData = kafkaStream.selectExpr("topic", "partition", "offset", "timestamp AS kafka_timestamp", "CAST(value AS STRING)")

val massagedData = rawData.select($"topic", $"partition", $"offset", $"kafka_timestamp", from_json($"value", KafkaMetadata.jsonSchema).as("value")).select($"topic", $"partition", $"offset", $"kafka_timestamp", $"value.database", $"value.table", $"value.position", $"value.ts", $"value.primary_key", $"value.data.*")
massagedData.createOrReplaceTempView("massagedData")

var query = 
	""" 
	  |SELECT
	  |		 database AS db
	  |		,table AS tbl
	  |		,CASE WHEN date_modified IS NOT NULL THEN date_modified ELSE last_updated END AS tbl_date_modified
	  |		,topic
	  |		,partition
	  |		,offset
	  |		,primary_key
	  |		,position AS binlog_position
	  |		,ts AS binlog_timestamp
	  |		,kafka_timestamp
	  |FROM massagedData
	""".stripMargin

// val q = sql(query).writeStream.format("console").outputMode("append").start
// q.stop

case class KafkaMetadata(db: String, tbl: String, tbl_date_modified: Timestamp, topic: String, partition: Int, offset: BigInt, 
	primary_key: String, binlog_position: String, binlog_timestamp: Timestamp, kafka_timestamp: Timestamp)

val kafkaMetadataWriter = new ForeachWriter[KafkaMetadata] {

  def open(partitionId: Long, version: Long): Boolean = true

  def process(value: KafkaMetadata): Unit = {
    val cQuery1 = s"""
    				 |INSERT INTO metadata.kafka_metadata (db, tbl, tbl_date_modified, topic, partition, offset, primary_key, binlog_position, kafka_timestamp, binlog_timestamp, meta_last_updated) 
    				 |VALUES ('${value.db}', '${value.tbl}', '${value.tbl_date_modified}', '${value.topic}', ${value.partition}, ${value.offset}, '${value.primary_key}', '${value.binlog_position}', '${value.kafka_timestamp}', '${value.binlog_timestamp}', toTimestamp(now()))
    			   """.stripMargin
    
    connector.withSessionDo { session => session.execute(cQuery1) }
  }

  def close(errorOrNull: Throwable): Unit = {}
}

val q = sql(query).as[KafkaMetadata].writeStream.foreach(kafkaMetadataWriter).outputMode("append").start



