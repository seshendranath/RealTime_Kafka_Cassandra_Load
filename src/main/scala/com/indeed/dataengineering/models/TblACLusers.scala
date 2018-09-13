package com.indeed.dataengineering.models

import java.sql.Timestamp
import org.apache.spark.sql.types._

case class TblACLusers(
                        topic: String
                        , partition: Int
                        , offset: BigInt
                        , opType: String
                        ,id: BigInt
                        ,firstname: String
                        ,lastname: String
                        ,email: String
                        ,username: String
                        ,active: Option[BigInt]
                        ,createdate: Timestamp
                        ,timezone: Option[Int]
                        ,entity: String
                        ,adp_file_number: Option[BigInt]
                        ,intacct_department_id: String
                        ,workday_id: String
                        ,workday_team: String
                        ,date_created: Timestamp
                        ,date_modified: Timestamp
                      )


object TblACLusers {
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
      ,StructField("firstname", StringType)
      ,StructField("lastname", StringType)
      ,StructField("email", StringType)
      ,StructField("username", StringType)
      ,StructField("active", LongType)
      ,StructField("createdate", TimestampType)
      ,StructField("timezone", IntegerType)
      ,StructField("entity", StringType)
      ,StructField("adp_file_number", LongType)
      ,StructField("intacct_department_id", StringType)
      ,StructField("workday_id", StringType)
      ,StructField("workday_team", StringType)
      ,StructField("date_created", TimestampType)
      ,StructField("date_modified", TimestampType)))),
    StructField("old", StructType(Array(
      StructField("id", LongType)
      ,StructField("firstname", StringType)
      ,StructField("lastname", StringType)
      ,StructField("email", StringType)
      ,StructField("username", StringType)
      ,StructField("active", LongType)
      ,StructField("createdate", TimestampType)
      ,StructField("timezone", IntegerType)
      ,StructField("entity", StringType)
      ,StructField("adp_file_number", LongType)
      ,StructField("intacct_department_id", StringType)
      ,StructField("workday_id", StringType)
      ,StructField("workday_team", StringType)
      ,StructField("date_created", TimestampType)
      ,StructField("date_modified", TimestampType))))
  ))
}
