package com.indeed.dataengineering.task

import org.apache.spark.sql._
import com.indeed.dataengineering.AnalyticsTaskApp._
import com.datastax.driver.core._
import collection.JavaConverters._

class TblADCsummary_advertiser_Initial_Load {

  def run(): Unit = {

    log.info("Setting spark.cassandra.connection.host")
    spark.conf.set("spark.cassandra.connection.host", conf("cassandra.host"))
    spark.conf.set("spark.sql.shuffle.partitions", conf("numPartitions"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.setInt("fs.s3a.connection.maximum", 1200)

    val cassandraHosts = conf("cassandra.host").split(",").toSeq
    val cluster = Cluster.builder().addContactPoints(cassandraHosts: _*).build()
    val session = cluster.connect("adcentraldb")

    val sql = spark.sql _

    val db = "adcentraldb"
    val tbl = "tblADCsummary_advertiser"

    val sourcePath = conf("sourcePath")
    log.info(s"Reading adcentraldb.tblADCsummary_advertiser from S3: $sourcePath")
    val df = spark.read.parquet(sourcePath).select("advertiser_id", "account_id", "masteraccount_id", "company", "date_created", "active", "monthly_budget", "monthly_budget_local", "expended_budget", "expended_budget_local", "lifetime_budget_local", "expended_lifetime_budget_local", "type", "payment_method", "billing_threshold", "billing_threshold_local", "industry", "is_ad_agency", "agency_discount", "process_level", "estimated_budget", "estimated_budget_local", "first_revenue_date", "last_revenue_date", "account_email", "billing_zip", "billing_state", "billing_country", "entity", "vat_country", "vat_number", "vat_exempt_13b_number", "agency_rep", "sales_rep", "service_rep", "strategic_rep", "sj_count", "hj_count", "hjr_count", "daily_budget_hits_days", "daily_budget_hits_ads", "predicted_spend", "predicted_spend_local", "showhostedjobs", "bid_managed", "companypagestatus", "sponjobindeedapply", "locale", "last_note_date", "last_email_date", "last_call_date", "last_meeting_date", "last_proposal_date", "last_activity_date", "parent_company_id", "currency", "nps_likely", "nps_notusing_reason", "commission_start_date", "commission_last_date", "employee_count", "timestamp_created", "date_modified", "is_deleted", "etl_file_inserted_timestamp")
    // df.repartition(conf("numPartitions").toInt)
    // df.persist
    // val insert_count = df.count
    // log.info(s"Total count of adcentraldb.tblADCsummary_advertiser from S3: $insert_count")
    df.createOrReplaceTempView("df")

    val query =
      """
        |SELECT
        |     activity_date,advertiser_id,account_id,masteraccount_id,company,date_created,active,monthly_budget,monthly_budget_local,expended_budget,expended_budget_local,lifetime_budget_local,expended_lifetime_budget_local,type,payment_method,billing_threshold,billing_threshold_local,industry,is_ad_agency,agency_discount,process_level,estimated_budget,estimated_budget_local,first_revenue_date,last_revenue_date,account_email,billing_zip,billing_state,billing_country,entity,vat_country,vat_number,vat_exempt_13b_number,agency_rep,sales_rep,service_rep,strategic_rep,sj_count,hj_count,hjr_count,daily_budget_hits_days,daily_budget_hits_ads,predicted_spend,predicted_spend_local,showhostedjobs,bid_managed,companypagestatus,sponjobindeedapply,locale,last_note_date,last_email_date,last_call_date,last_meeting_date,last_proposal_date,last_activity_date,parent_company_id,currency,nps_likely,nps_notusing_reason,commission_start_date,commission_last_date,employee_count,timestamp_created,date_modified,is_deleted,binlog_timestamp
        |FROM
        |(SELECT
        |     *
        |    ,CAST(COALESCE(date_modified, etl_file_inserted_timestamp) AS DATE) AS activity_date
        |    ,COALESCE(date_modified, etl_file_inserted_timestamp) AS binlog_timestamp
        |    ,row_number() OVER (PARTITION BY CAST(COALESCE(date_modified, etl_file_inserted_timestamp) AS DATE), advertiser_id ORDER BY COALESCE(date_modified, etl_file_inserted_timestamp) DESC) AS rank
        |FROM df) a
        |WHERE rank = 1 AND activity_date IS NOT NULL
      """.stripMargin

    val updatedDF = sql(query).repartition(conf("numPartitions").toInt)
    updatedDF.persist()
    val insert_count = updatedDF.count
    log.info(s"updatedDF Count: $insert_count")
    updatedDF.createOrReplaceTempView("updatedDF")

    log.info("Writing Data to Cassandra")
    updatedDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> tbl.toLowerCase, "keyspace" -> db)).save
    log.info("Successfully written Data to Cassandra")

    log.info("Fetching Max binlog_timestamp from updatedDF")
    val latest_dt_modified = sql("SELECT MAX(binlog_timestamp) FROM updatedDF").collect.head.get(0).toString.take(19)
    log.info(s"latest_dt_modified from updatedDF: $latest_dt_modified")

    var cQuery = s"SELECT MIN(topic) AS topic, MIN(partition) AS partition, MIN(offset) AS offset FROM metadata.kafka_metadata WHERE db = '$db' AND tbl = '$tbl' AND tbl_date_modified = '$latest_dt_modified'"

    log.info(s"Running Query: $cQuery in Cassandra")
    val res = session.execute(cQuery).all.asScala.toArray.head
    val (topic, partition, offset) = (res.getString("topic"), res.getInt("partition"), res.getLong("offset"))

    if (topic == null) log.warn(s"Topic is NULL. Please check the ")
    log.info(s"Topic, Partition, and offset results: $topic, $partition, $offset")

    cQuery = s"INSERT INTO metadata.streaming_metadata (job, db, tbl, topic, partition, offset) VALUES('${tbl.capitalize + "_Load"}', '$db', '$tbl', '$topic', $partition, $offset)"
    log.info(s"Running Query: $cQuery in Cassandra")
    session.execute(cQuery)

    cQuery = s"UPDATE stats.streaming_stats SET inserted_records = inserted_records + $insert_count WHERE job = '${tbl.capitalize + "_Load"}' AND db = '$db' AND tbl = '$tbl'"
    log.info(s"Running Query: $cQuery in Cassandra")
    session.execute(cQuery)

    log.info(s"Successfully Loaded all data of adcentraldb.tblADCsummary_advertiser from S3 to Cassandra and updated metadata and stats!")
  }

}
