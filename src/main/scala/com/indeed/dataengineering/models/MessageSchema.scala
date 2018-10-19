package com.indeed.dataengineering.models

import org.apache.spark.sql.types._

object MessageSchema {
  val jsonSchema = StructType(Array(
    StructField("database", StringType),
    StructField("table", StringType),
    StructField("type", StringType),
    StructField("ts", TimestampType),
    StructField("position", StringType),
    StructField("primary_key", StringType),
    StructField("data", StringType),
    StructField("old", StringType)
  ))
}