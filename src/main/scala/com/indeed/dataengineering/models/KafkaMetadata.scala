package com.indeed.dataengineering.models

import java.sql.Timestamp
import org.apache.spark.sql.types._

case class KafkaMetadata(db: String, tbl: String, tbl_date_modified: Timestamp, topic: String, partition: Int, offset: BigInt,
                         primary_key: String, binlog_position: String, binlog_timestamp: Timestamp, kafka_timestamp: Timestamp)


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