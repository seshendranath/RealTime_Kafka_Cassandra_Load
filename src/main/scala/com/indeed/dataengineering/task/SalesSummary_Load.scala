package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.{log => _, _}


class SalesSummary_Load {

  def run(): Unit = {

    import spark.implicits._

    val sql = spark.sql _

    val checkpointDir = conf("checkpointBaseLoc") + this.getClass.getSimpleName

    val Array(brokers, topics) = Array(conf("kafka.brokers"), conf("kafka.topic"))
    log.info(s"Initialized the Kafka brokers and topics to $brokers and $topics")

    log.info("Setting spark.cassandra.connection.host")
    spark.conf.set("spark.cassandra.connection.host", conf("cassandra.host"))

    log.info(s"Create Cassandra connector by passing host as ${conf("cassandra.host")}")
    val connector = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", conf("cassandra.host")))

    log.info("Read Kafka streams")
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics).option("failOnDataLoss", "false")
      .load()
    //.option("startingOffsets", s""" {"${conf("kafka.topic")}":{"0":-1}} """)

    log.info("Extract value and map from Kafka consumer records")
    val rawData = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map(_._2)


    log.info("Defining new ForeachWriter for Sales_Revenue_Summary")
    val Sales_Revenue_SummaryWriter = new ForeachWriter[Sales_Revenue_Summary] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: Sales_Revenue_Summary): Unit = {
        val total_revenue = value.sales_revenue + value.agency_revenue + value.strategic_revenue + value.sales_new_revenue
        val cQuery1 = s"update adcentraldb.sales_revenue_summary_by_user_quarter SET total_revenue = total_revenue + $total_revenue, sales_revenue = sales_revenue + ${value.sales_revenue}, agency_revenue = agency_revenue + ${value.agency_revenue}, strategic_revenue = strategic_revenue + ${value.strategic_revenue}, sales_new_revenue = sales_new_revenue + ${value.sales_new_revenue} WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}"
        val cQuery2 = s"update adcentraldb.sales_revenue_summary_by_quarter SET total_revenue = total_revenue + $total_revenue, sales_revenue = sales_revenue + ${value.sales_revenue}, agency_revenue = agency_revenue + ${value.agency_revenue}, strategic_revenue = strategic_revenue + ${value.strategic_revenue}, sales_new_revenue = sales_new_revenue + ${value.sales_new_revenue} WHERE year = ${value.year} AND quarter = ${value.quarter}"
        val cQuery3 = s"SELECT total_revenue FROM adcentraldb.sales_revenue_summary_by_user_quarter WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}"
        val cQuery4 = s"SELECT total_revenue FROM adcentraldb.sales_revenue_summary_by_quarter WHERE year = ${value.year} AND quarter = ${value.quarter}"

        connector.withSessionDo { session =>
          session.execute(cQuery1)
          session.execute(cQuery2)
          val cRow1 = session.execute(cQuery3).one
          val total_revenue_user_quarter = if (cRow1 != null && cRow1.getObject("total_revenue") != null) BigInt(cRow1.getObject("total_revenue").toString) else BigInt(0)
          session.execute(s"update adcentraldb.sales_revenue_quota_summary_by_user_quarter SET total_revenue = $total_revenue_user_quarter WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}")

          val cRow2 = session.execute(cQuery4).one
          val total_revenue_quarter = if (cRow2 != null && cRow2.getObject("total_revenue") != null) BigInt(cRow2.getObject("total_revenue").toString) else BigInt(0)
          session.execute(s"update adcentraldb.sales_revenue_quota_summary_by_quarter SET total_revenue = $total_revenue_quarter WHERE year = ${value.year} AND quarter = ${value.quarter}")

        }
      }

      def close(errorOrNull: Throwable): Unit = {}
    }

    log.info("Reading adsystemdb.testadvertiserids from cassandra")
    val staticTestAdvertiserIds = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "testadvertiserids", "keyspace" -> "adsystemdb")).load.repartition(1)
    staticTestAdvertiserIds.persist
    log.info(s"Count of adsystemdb.testadvertiserids from cassandra: ${staticTestAdvertiserIds.count}")
    staticTestAdvertiserIds.createOrReplaceTempView("staticTestAdvertiserIds")

    log.info("Reading tbladvertiser from kafka streams")
    val tblAdvertiser = rawData.select(from_json($"value", Tbladvertiser.jsonSchema).as("value")).filter($"value.table" === "tbladvertiser").select($"value.type".as("opType"), $"value.data.*")
    val streamTestAdvertiserIds = tblAdvertiser.where("opType IN ('insert', 'update', 'delete') AND type='Test'").selectExpr("CAST(id AS BIGINT)").distinct

    log.info("Constructing in memory table of test advertiser ids")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      streamTestAdvertiserIds.writeStream.option("checkpointLocation", checkpointDir + "/streamTestAdvertiserIds").queryName("streamTestAdvertiserIds").outputMode("append").format("memory").start()
    } else {
      streamTestAdvertiserIds.writeStream.queryName("streamTestAdvertiserIds").outputMode("append").format("memory").start()
    }



    log.info("Reading adsystemdb.tbladscurrency_rates from cassandra")
    val statictblADScurrency_rates = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladscurrency_rates", "keyspace" -> "adsystemdb")).load.select("activity_date", "from_currency", "exchange_rate").distinct.repartition(1)
    statictblADScurrency_rates.persist
    log.info(s"Count of adsystemdb.tbladscurrency_rates from cassandra: ${statictblADScurrency_rates.count}")
    statictblADScurrency_rates.createOrReplaceTempView("statictblADScurrency_rates")

    log.info("Reading tblADScurrency_rates from kafka streams")
    val tblADScurrency_rates = rawData.select(from_json($"value", TblADScurrency_rates.jsonSchema).as("value")).filter($"value.table" === "tblADScurrency_rates").select($"value.type".as("opType"), $"value.data.*")
    val streamtblADScurrency_rates = tblADScurrency_rates.where("opType IN ('insert', 'update')").select("activity_date", "from_currency", "exchange_rate").distinct

    log.info("Constructing in memory table of tblADScurrency_rates")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      streamtblADScurrency_rates.writeStream.option("checkpointLocation", checkpointDir + "/streamtblADScurrency_rates").queryName("streamtblADScurrency_rates").outputMode("append").format("memory").start()
    } else {
      streamtblADScurrency_rates.writeStream.queryName("streamtblADScurrency_rates").outputMode("append").format("memory").start()
    }



    log.info("Reading tblADCaccounts_salesrep_commissions from kafka streams")
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
        |FROM tblADCaccounts_salesrep_commissions  a
        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
      """.stripMargin

    log.info(s"Running Streaming Query: $tblADCaccounts_salesrep_commissionsSummarySql")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      sql(tblADCaccounts_salesrep_commissionsSummarySql).as[Sales_Revenue_Summary].writeStream.option("checkpointLocation", checkpointDir + "/tblADCaccounts_salesrep_commissionsSummarySql").foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    } else {
      sql(tblADCaccounts_salesrep_commissionsSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    }



    val tblADCaccounts_salesrep_commissionsSummaryDebugSql =
      """
        |SELECT
        |      YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,user_id
        |     ,opType
        |     ,revenue_jobsearch_millicents
        |     ,revenue_resume_millicents
        |     ,revenue_dradis_lifetime_millicents
        |     ,revenue_dradis_recurring_millicents
        |     ,old_revenue_jobsearch_millicents
        |     ,old_revenue_resume_millicents
        |     ,old_revenue_dradis_lifetime_millicents
        |     ,old_revenue_dradis_recurring_millicents
        |     ,newrevenue_jobsearch_millicents
        |     ,newrevenue_resume_millicents
        |     ,newrevenue_dradis_lifetime_millicents
        |     ,newrevenue_dradis_recurring_millicents
        |     ,old_newrevenue_jobsearch_millicents
        |     ,old_newrevenue_resume_millicents
        |     ,old_newrevenue_dradis_lifetime_millicents
        |     ,old_newrevenue_dradis_recurring_millicents
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
        |FROM tblADCaccounts_salesrep_commissions a
        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
      """.stripMargin

    log.info(s"Running Streaming Query: $tblADCaccounts_salesrep_commissionsSummaryDebugSql")
    if (conf.getOrElse("debug", "false") == "true") sql(tblADCaccounts_salesrep_commissionsSummaryDebugSql).as[Sales_Revenue_Summary].writeStream.format("console").outputMode("append").start()

    val tblADCaccounts_salesrep_commissionsSummaryDebugSql1 =
      """
        |SELECT
        |      count(*) AS cnt
        |     ,SUM(CASE WHEN opType = 'insert' THEN
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
        |           END) AS sales_revenue
        |       ,SUM(CASE WHEN opType = 'insert' THEN
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
        |           END)  AS sales_new_revenue
        |FROM tblADCaccounts_salesrep_commissions a
        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
      """.stripMargin

    log.info(s"Running Streaming Query: $tblADCaccounts_salesrep_commissionsSummaryDebugSql1")
    if (conf.getOrElse("debug", "false") == "true") sql(tblADCaccounts_salesrep_commissionsSummaryDebugSql1).writeStream.format("console").outputMode("complete").start()


    log.info("Reading tblCRMgeneric_product_credit from kafka streams")
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

    log.info(s"Running Streaming Query: $tblCRMgeneric_product_creditSummarySql")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      sql(tblCRMgeneric_product_creditSummarySql).as[Sales_Revenue_Summary].writeStream.option("checkpointLocation", checkpointDir + "/tblCRMgeneric_product_creditSummarySql").foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    } else {
      sql(tblCRMgeneric_product_creditSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    }



    log.info("Reading tblADCadvertiser_rep_revenues from kafka streams")
    val tblADCadvertiser_rep_revenues = rawData.select(from_json($"value", TblADCadvertiser_rep_revenues.jsonSchema).as("value")).filter($"value.table" === "tblADCadvertiser_rep_revenues").select($"value.type".as("opType")
      , $"value.data.activity_date".as("date")
      , $"value.data.user_id"
      , $"value.data.advertiser_id"
      , $"value.data.relationship"
      , $"value.data.revenue_jobsearch_millicents"
      , $"value.data.revenue_resume_millicents"
      , $"value.old.revenue_jobsearch_millicents".as("old_revenue_jobsearch_millicents")
      , $"value.old.revenue_resume_millicents".as("old_revenue_resume_millicents")).where("opType IN ('insert', 'update', 'delete') AND relationship IN ('AGENCY_REP', 'STRATEGIC_REP')")

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
        |FROM tblADCadvertiser_rep_revenues  a
        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
        |)
      """.stripMargin

    log.info(s"Running Streaming Query: $tblADCadvertiser_rep_revenuesSummarySql")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      sql(tblADCadvertiser_rep_revenuesSummarySql).as[Sales_Revenue_Summary].writeStream.option("checkpointLocation", checkpointDir + "/tblADCadvertiser_rep_revenuesSummarySql").foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    } else {
      sql(tblADCadvertiser_rep_revenuesSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    }



    log.info("Await Any Stream Query Termination")
    spark.streams.awaitAnyTermination
  }
}
