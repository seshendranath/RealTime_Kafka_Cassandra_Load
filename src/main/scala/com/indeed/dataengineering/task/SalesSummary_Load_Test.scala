package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import com.indeed.dataengineering.AnalyticsTaskApp._
import org.apache.spark.sql._
import com.indeed.dataengineering.models._
import com.datastax.spark.connector.cql.CassandraConnector
import com.indeed.dataengineering.utilities.Logging
import com.indeed.dataengineering.utilities.Utils.getMetaQueries
import org.apache.spark.sql.functions.{log => _, _}


class SalesSummary_Load_Test  extends Logging {

  def run(rawData: DataFrame, connector: CassandraConnector, partitions: Set[Int]): Unit = {

    import spark.implicits._

    val sql = spark.sql _

    val checkpointDir = conf("checkpointBaseLoc") + this.getClass.getSimpleName

    val className = this.getClass.getSimpleName

    log.info("Setting spark.cassandra.connection.host")
    spark.conf.set("spark.cassandra.connection.host", conf("cassandra.host"))

    log.info("Defining new ForeachWriter for Sales_Revenue_Summary")
    val Sales_Revenue_SummaryWriter = new ForeachWriter[Sales_Revenue_Summary] {

      def open(partitionId: Long, version: Long): Boolean = true

      def process(value: Sales_Revenue_Summary): Unit = {

        val metaQuery = getMetaQueries(className, value.db, value.tbl, value.topic, value.partition, value.offset)

        val total_revenue = value.sales_revenue + value.agency_revenue + value.strategic_revenue + value.sales_new_revenue
//        val cQuery1 = s"update adcentraldb.sales_revenue_summary_by_user_quarter_test SET total_revenue = total_revenue + $total_revenue, sales_revenue = sales_revenue + ${value.sales_revenue}, agency_revenue = agency_revenue + ${value.agency_revenue}, strategic_revenue = strategic_revenue + ${value.strategic_revenue}, sales_new_revenue = sales_new_revenue + ${value.sales_new_revenue}, new_parent_revenue = new_parent_revenue + ${value.new_parent_revenue}  WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}"
//        val cQuery2 = s"update adcentraldb.sales_revenue_summary_by_quarter_test SET total_revenue = total_revenue + $total_revenue, sales_revenue = sales_revenue + ${value.sales_revenue}, agency_revenue = agency_revenue + ${value.agency_revenue}, strategic_revenue = strategic_revenue + ${value.strategic_revenue}, sales_new_revenue = sales_new_revenue + ${value.sales_new_revenue}, new_parent_revenue = new_parent_revenue + ${value.new_parent_revenue} WHERE year = ${value.year} AND quarter = ${value.quarter}"
//        val cQuery3 = s"SELECT total_revenue FROM adcentraldb.sales_revenue_summary_by_user_quarter WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}"
//        val cQuery4 = s"SELECT total_revenue FROM adcentraldb.sales_revenue_summary_by_quarter WHERE year = ${value.year} AND quarter = ${value.quarter}"

        connector.withSessionDo { session =>
//          session.execute(cQuery1)
//          session.execute(cQuery2)
//          session.execute(metaQuery)
//          val cRow1 = session.execute(cQuery3).one
//          val total_revenue_user_quarter = if (cRow1 != null && cRow1.getObject("total_revenue") != null) BigInt(cRow1.getObject("total_revenue").toString) else BigInt(0)
//          session.execute(s"update adcentraldb.sales_revenue_quota_summary_by_user_quarter SET total_revenue = $total_revenue_user_quarter WHERE year = ${value.year} AND quarter = ${value.quarter} AND user_id = ${value.user_id}")

//          val cRow2 = session.execute(cQuery4).one
//          val total_revenue_quarter = if (cRow2 != null && cRow2.getObject("total_revenue") != null) BigInt(cRow2.getObject("total_revenue").toString) else BigInt(0)
//          session.execute(s"update adcentraldb.sales_revenue_quota_summary_by_quarter SET total_revenue = $total_revenue_quarter WHERE year = ${value.year} AND quarter = ${value.quarter}")

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

    // Seq((1, 1, "2018-06-01")).toDF("id", "user_id", "first_revenue_date") //
    log.info("Reading adcentraldb.tbladcparent_company from cassandra")
    val staticTbladcparent_company = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcparent_company", "keyspace" -> "adcentraldb")).load.select("id", "user_id", "first_revenue_date").where("first_revenue_date >= '2018-01-01'").repartition(5)
    staticTbladcparent_company.persist
    log.info(s"Count of adcentraldb.tbladcparent_company from cassandra: ${staticTbladcparent_company.count}")
    staticTbladcparent_company.createOrReplaceTempView("staticTbladcparent_company")

    log.info("Reading tbladcparent_company from kafka streams")
    val tbladcparent_company = rawData.select(from_json($"value", TblADCparent_company.jsonSchema).as("value")).filter($"value.table" === "tblADCparent_company").select($"value.type".as("opType"), $"value.data.id", $"value.data.user_id", $"value.data.first_revenue_date")
    val streamTbladcparent_company = tbladcparent_company.where("opType IN ('insert', 'update')").select("id", "user_id", "first_revenue_date").where("first_revenue_date >= '2018-01-01'").distinct

    log.info("Constructing in memory table of tbladcparent_company")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      streamTbladcparent_company.writeStream.option("checkpointLocation", checkpointDir + "/streamTbladcparent_company").queryName("streamTbladcparent_company").outputMode("append").format("memory").start()
    } else {
      streamTbladcparent_company.writeStream.queryName("streamTbladcparent_company").outputMode("append").format("memory").start()
    }

    // Seq((1, 1)).toDF("parent_company_id", "advertiser_id") //
    log.info("Reading adcentraldb.tbladcparent_company_advertisers from cassandra")
    val staticTbladcparent_company_advertisers = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcparent_company_advertisers", "keyspace" -> "adcentraldb")).load.select("parent_company_id", "advertiser_id").repartition(5)
    staticTbladcparent_company_advertisers.persist
    log.info(s"Count of adcentraldb.tbladcparent_company_advertisers from cassandra: ${staticTbladcparent_company_advertisers.count}")
    staticTbladcparent_company_advertisers.createOrReplaceTempView("staticTbladcparent_company_advertisers")

    log.info("Reading tbladcparent_company_advertisers from kafka streams")
    val tbladcparent_company_advertisers = rawData.select(from_json($"value", TblADCparent_company_advertisers.jsonSchema).as("value")).filter($"value.table" === "tblADCparent_company_advertisers").select($"value.type".as("opType"), $"value.data.parent_company_id", $"value.data.advertiser_id")
    val streamTbladcparent_company_advertisers = tbladcparent_company_advertisers.where("opType IN ('insert', 'update')").select("parent_company_id", "advertiser_id").distinct

    log.info("Constructing in memory table of tbladcparent_company_advertisers")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      streamTbladcparent_company_advertisers.writeStream.option("checkpointLocation", checkpointDir + "/streamTbladcparent_company_advertisers").queryName("streamTbladcparent_company_advertisers").outputMode("append").format("memory").start()
    } else {
      streamTbladcparent_company_advertisers.writeStream.queryName("streamTbladcparent_company_advertisers").outputMode("append").format("memory").start()
    }

    // Seq(1).toDF("id") //
    log.info("Reading adcentraldb.tblaclusers from cassandra")
    val staticTblaclusers = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tblaclusers", "keyspace" -> "adcentraldb")).load.where("entity != 'US' AND workday_team like '%NAM%'").select("id").distinct.repartition(1)
    staticTblaclusers.persist
    log.info(s"Count of adcentraldb.tblaclusers from cassandra: ${staticTblaclusers.count}")
    staticTblaclusers.createOrReplaceTempView("staticTblaclusers")

    log.info("Reading tblaclusers from kafka streams")
    val tblaclusers = rawData.select(from_json($"value", TblACLusers.jsonSchema).as("value")).filter($"value.table" === "tblACLusers").select($"value.type".as("opType"), $"value.data.id", $"value.data.entity", $"value.data.workday_team")
    val streamTblaclusers = tblaclusers.where("opType IN ('insert', 'update') AND entity != 'US' AND workday_team like '%NAM%'").select("id").distinct

    log.info("Constructing in memory table of tblaclusers")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      streamTblaclusers.writeStream.option("checkpointLocation", checkpointDir + "/streamTblaclusers").queryName("streamTblaclusers").outputMode("append").format("memory").start()
    } else {
      streamTblaclusers.writeStream.queryName("streamTblaclusers").outputMode("append").format("memory").start()
    }


    log.info("Running New Parent Query")
    val newParentQuery =
      """
        |SELECT
        |      pc.id
        |     ,pca.advertiser_id
        |     ,pc.user_id
        |     ,pc.first_revenue_date
        |FROM
        |(
        |SELECT tmpPC.*
        |FROM
        |(SELECT * FROM staticTbladcparent_company UNION SELECT * FROM streamTbladcparent_company) tmpPC
        |INNER JOIN (SELECT * FROM staticTblaclusers UNION SELECT * FROM streamTblaclusers) u ON tmpPC.user_id = u.id
        |) pc
        |INNER JOIN (SELECT * FROM staticTbladcparent_company_advertisers UNION SELECT * FROM streamTbladcparent_company_advertisers) pca ON pc.id = pca.parent_company_id
      """.stripMargin

    sql(newParentQuery).createOrReplaceTempView("newParent")


    log.info("Reading tblADCaccounts_salesrep_commissions from kafka streams")
    val tblADCaccounts_salesrep_commissions = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCaccounts_salesrep_commissions.jsonSchema).as("value")).filter($"value.table" === "tblADCaccounts_salesrep_commissions").select($"topic", $"partition", $"offset", $"value.type".as("opType")
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
        |      "adcentraldb" AS db
        |     ,"tblADCaccounts_salesrep_commissions" AS tbl
        |     ,topic
        |     ,partition
        |     ,offset
        |     ,YEAR(date) AS year
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
        |     ,0 AS new_parent_revenue
        |FROM tblADCaccounts_salesrep_commissions  a
        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
        |UNION ALL
        |SELECT
        |      "adcentraldb" AS db
        |     ,"tblADCaccounts_salesrep_commissions" AS tbl
        |     ,topic
        |     ,partition
        |     ,offset
        |     ,YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,accsc.user_id AS user_id
        |     ,0 AS sales_revenue
        |     ,0 AS agency_revenue
        |     ,0 AS strategic_revenue
        |     ,0 AS sales_new_revenue
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
        |           END AS new_parent_revenue
        |FROM tblADCaccounts_salesrep_commissions accsc
        |INNER JOIN newParent np ON accsc.advertiser_id = np.advertiser_id AND accsc.user_id = np.user_id
        |WHERE accsc.date <= DATE_ADD(np.first_revenue_date, 89)
      """.stripMargin

    log.info(s"Running Streaming Query: $tblADCaccounts_salesrep_commissionsSummarySql")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      sql(tblADCaccounts_salesrep_commissionsSummarySql).as[Sales_Revenue_Summary].writeStream.option("checkpointLocation", checkpointDir + "/tblADCaccounts_salesrep_commissionsSummarySql").foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    } else {
      sql(tblADCaccounts_salesrep_commissionsSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    }



    //    val tblADCaccounts_salesrep_commissionsSummaryDebugSql =
    //      """
    //        |SELECT
    //        |      YEAR(date) AS year
    //        |     ,QUARTER(date) AS quarter
    //        |     ,user_id
    //        |     ,opType
    //        |     ,revenue_jobsearch_millicents
    //        |     ,revenue_resume_millicents
    //        |     ,revenue_dradis_lifetime_millicents
    //        |     ,revenue_dradis_recurring_millicents
    //        |     ,old_revenue_jobsearch_millicents
    //        |     ,old_revenue_resume_millicents
    //        |     ,old_revenue_dradis_lifetime_millicents
    //        |     ,old_revenue_dradis_recurring_millicents
    //        |     ,newrevenue_jobsearch_millicents
    //        |     ,newrevenue_resume_millicents
    //        |     ,newrevenue_dradis_lifetime_millicents
    //        |     ,newrevenue_dradis_recurring_millicents
    //        |     ,old_newrevenue_jobsearch_millicents
    //        |     ,old_newrevenue_resume_millicents
    //        |     ,old_newrevenue_dradis_lifetime_millicents
    //        |     ,old_newrevenue_dradis_recurring_millicents
    //        |     ,CASE WHEN opType = 'insert' THEN
    //        |                                       COALESCE(revenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(revenue_resume_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_recurring_millicents, 0)
    //        |           WHEN opType = 'update' THEN
    //        |                                       (COALESCE(revenue_jobsearch_millicents, 0 ) - COALESCE(old_revenue_jobsearch_millicents, revenue_jobsearch_millicents)) +
    //        |                                       (COALESCE(revenue_resume_millicents, 0 ) - COALESCE(old_revenue_resume_millicents, revenue_resume_millicents)) +
    //        |                                       (COALESCE(revenue_dradis_lifetime_millicents, 0 ) - COALESCE(old_revenue_dradis_lifetime_millicents, revenue_dradis_lifetime_millicents)) +
    //        |                                       (COALESCE(revenue_dradis_recurring_millicents, 0 ) - COALESCE(old_revenue_dradis_recurring_millicents, revenue_dradis_recurring_millicents))
    //        |           ELSE
    //        |                                       -(COALESCE(revenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(revenue_resume_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_recurring_millicents, 0))
    //        |           END AS sales_revenue
    //        |     ,0 AS agency_revenue
    //        |     ,0 AS strategic_revenue
    //        |     ,CASE WHEN opType = 'insert' THEN
    //        |                                       COALESCE(newrevenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_recurring_millicents, 0) +
    //        |                                       COALESCE(newrevenue_resume_millicents, 0)
    //        |           WHEN opType = 'update' THEN
    //        |                                       (COALESCE(newrevenue_jobsearch_millicents, 0 ) - COALESCE(old_newrevenue_jobsearch_millicents, newrevenue_jobsearch_millicents)) +
    //        |                                       (COALESCE(newrevenue_dradis_lifetime_millicents, 0 ) - COALESCE(old_newrevenue_dradis_lifetime_millicents, newrevenue_dradis_lifetime_millicents)) +
    //        |                                       (COALESCE(newrevenue_dradis_recurring_millicents, 0 ) - COALESCE(old_newrevenue_dradis_recurring_millicents, newrevenue_dradis_recurring_millicents)) +
    //        |                                       (COALESCE(newrevenue_resume_millicents, 0 ) - COALESCE(old_newrevenue_resume_millicents, newrevenue_resume_millicents))
    //        |           ELSE
    //        |                                       -(COALESCE(newrevenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_recurring_millicents, 0) +
    //        |                                       COALESCE(newrevenue_resume_millicents, 0))
    //        |           END AS sales_new_revenue
    //        |FROM tblADCaccounts_salesrep_commissions a
    //        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
    //        |ON a.advertiser_id = b.id
    //        |WHERE b.id IS NULL
    //      """.stripMargin
    //
    //    log.info(s"Running Streaming Query: $tblADCaccounts_salesrep_commissionsSummaryDebugSql")
    //    if (conf.getOrElse("debug", "false") == "true") sql(tblADCaccounts_salesrep_commissionsSummaryDebugSql).as[Sales_Revenue_Summary].writeStream.format("console").outputMode("append").start()
    //
    //    val tblADCaccounts_salesrep_commissionsSummaryDebugSql1 =
    //      """
    //        |SELECT
    //        |      count(*) AS cnt
    //        |     ,SUM(CASE WHEN opType = 'insert' THEN
    //        |                                       COALESCE(revenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(revenue_resume_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_recurring_millicents, 0)
    //        |           WHEN opType = 'update' THEN
    //        |                                       (COALESCE(revenue_jobsearch_millicents, 0 ) - COALESCE(old_revenue_jobsearch_millicents, revenue_jobsearch_millicents)) +
    //        |                                       (COALESCE(revenue_resume_millicents, 0 ) - COALESCE(old_revenue_resume_millicents, revenue_resume_millicents)) +
    //        |                                       (COALESCE(revenue_dradis_lifetime_millicents, 0 ) - COALESCE(old_revenue_dradis_lifetime_millicents, revenue_dradis_lifetime_millicents)) +
    //        |                                       (COALESCE(revenue_dradis_recurring_millicents, 0 ) - COALESCE(old_revenue_dradis_recurring_millicents, revenue_dradis_recurring_millicents))
    //        |           ELSE
    //        |                                       -(COALESCE(revenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(revenue_resume_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(revenue_dradis_recurring_millicents, 0))
    //        |           END) AS sales_revenue
    //        |       ,SUM(CASE WHEN opType = 'insert' THEN
    //        |                                       COALESCE(newrevenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_recurring_millicents, 0) +
    //        |                                       COALESCE(newrevenue_resume_millicents, 0)
    //        |           WHEN opType = 'update' THEN
    //        |                                       (COALESCE(newrevenue_jobsearch_millicents, 0 ) - COALESCE(old_newrevenue_jobsearch_millicents, newrevenue_jobsearch_millicents)) +
    //        |                                       (COALESCE(newrevenue_dradis_lifetime_millicents, 0 ) - COALESCE(old_newrevenue_dradis_lifetime_millicents, newrevenue_dradis_lifetime_millicents)) +
    //        |                                       (COALESCE(newrevenue_dradis_recurring_millicents, 0 ) - COALESCE(old_newrevenue_dradis_recurring_millicents, newrevenue_dradis_recurring_millicents)) +
    //        |                                       (COALESCE(newrevenue_resume_millicents, 0 ) - COALESCE(old_newrevenue_resume_millicents, newrevenue_resume_millicents))
    //        |           ELSE
    //        |                                       -(COALESCE(newrevenue_jobsearch_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_lifetime_millicents, 0) +
    //        |                                       COALESCE(newrevenue_dradis_recurring_millicents, 0) +
    //        |                                       COALESCE(newrevenue_resume_millicents, 0))
    //        |           END)  AS sales_new_revenue
    //        |FROM tblADCaccounts_salesrep_commissions a
    //        |LEFT OUTER JOIN (SELECT id FROM staticTestAdvertiserIds UNION SELECT id FROM streamTestAdvertiserIds) b
    //        |ON a.advertiser_id = b.id
    //        |WHERE b.id IS NULL
    //      """.stripMargin
    //
    //    log.info(s"Running Streaming Query: $tblADCaccounts_salesrep_commissionsSummaryDebugSql1")
    //    if (conf.getOrElse("debug", "false") == "true") sql(tblADCaccounts_salesrep_commissionsSummaryDebugSql1).writeStream.format("console").outputMode("complete").start()

    log.info("Reading tblCRMgeneric_product_credit from kafka streams")
    val tblCRMgeneric_product_credit = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblCRMgeneric_product_credit.jsonSchema).as("value")).filter($"value.table" === "tblCRMgeneric_product_credit").select($"topic", $"partition", $"offset", $"value.type".as("opType")
      , $"value.data.activity_date".as("date")
      , $"value.data.user_id"
      , $"value.data.advertiser_id"
      , $"value.data.relationship"
      , $"value.data.currency"
      , $"value.data.rejected"
      , $"value.data.revenue_generic_product_local"
      , $"value.old.revenue_generic_product_local".as("old_revenue_generic_product_local")).where("opType IN ('insert', 'update', 'delete') AND relationship in ('SALES_REP', 'STRATEGIC_REP', 'AGENCY_REP') AND rejected = 0")

    tblCRMgeneric_product_credit.createOrReplaceTempView("tblCRMgeneric_product_credit")

    val tblCRMgeneric_product_creditSummarySql =
      """
        |SELECT
        |      "adcentraldb" AS db
        |     ,"tblCRMgeneric_product_credit" AS tbl
        |     ,topic
        |     ,partition
        |     ,offset
        |     ,YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,user_id
        |     ,CASE WHEN relationship IN ('SALES_REP', 'STRATEGIC_REP') THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END AS sales_revenue
        |     ,CASE WHEN relationship = 'AGENCY_REP' THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END AS agency_revenue
        |     ,0 AS strategic_revenue
        |     ,0 AS sales_new_revenue
        |     ,0 AS new_parent_revenue
        |FROM
        |(SELECT
        |      topic
        |     ,partition
        |     ,offset
        |     ,date
        |     ,user_id
        |     ,currency
        |     ,relationship
        |     ,CASE WHEN opType = 'insert' THEN COALESCE(revenue_generic_product_local, 0)
        |           WHEN opType = 'update' THEN COALESCE(revenue_generic_product_local, 0 ) - COALESCE(old_revenue_generic_product_local, revenue_generic_product_local)
        |           ELSE -COALESCE(revenue_generic_product_local, 0)
        |           END AS total_local
        |FROM tblCRMgeneric_product_credit
        |) arr
        |INNER JOIN (SELECT * FROM statictblADScurrency_rates UNION SELECT * FROM streamtblADScurrency_rates) er
        |ON arr.date = er.activity_date AND arr.currency = er.from_currency
        |UNION ALL
        |SELECT
        |      "adcentraldb" AS db
        |     ,"tblCRMgeneric_product_credit" AS tbl
        |     ,topic
        |     ,partition
        |     ,offset
        |     ,YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,user_id
        |     ,0 AS sales_revenue
        |     ,0 AS agency_revenue
        |     ,0 AS strategic_revenue
        |     ,0 AS sales_new_revenue
        |     ,CAST(((total_local * exchange_rate) / 1000) AS BIGINT) AS new_parent_revenue
        |FROM
        |(SELECT
        |       topic
        |      ,partition
        |      ,offset
        |      ,tpc.user_id
        |      ,tpc.date
        |      ,CASE WHEN opType = 'insert' THEN COALESCE(revenue_generic_product_local, 0)
        |           WHEN opType = 'update' THEN COALESCE(revenue_generic_product_local, 0 ) - COALESCE(old_revenue_generic_product_local, revenue_generic_product_local)
        |           ELSE -COALESCE(revenue_generic_product_local, 0)
        |           END AS total_local
        |      ,tpc.CURRENCY
        |FROM tblCRMgeneric_product_credit tpc
        |INNER JOIN newParent np ON tpc.advertiser_id = np.advertiser_id AND tpc.user_id = np.user_id
        |WHERE tpc.RELATIONSHIP IN ('SALES_REP', 'STRATEGIC_REP')
        |       AND tpc.date <= DATE_ADD(np.first_revenue_date, 89)
        |) a
        |INNER JOIN (SELECT * FROM statictblADScurrency_rates UNION SELECT * FROM streamtblADScurrency_rates) r
        |ON a.date = r.activity_date AND a.CURRENCY = r.from_currency
      """.stripMargin

    log.info(s"Running Streaming Query: $tblCRMgeneric_product_creditSummarySql")
    if (conf.getOrElse("checkpoint", "false") == "true") {
      sql(tblCRMgeneric_product_creditSummarySql).as[Sales_Revenue_Summary].writeStream.option("checkpointLocation", checkpointDir + "/tblCRMgeneric_product_creditSummarySql").foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    } else {
      sql(tblCRMgeneric_product_creditSummarySql).as[Sales_Revenue_Summary].writeStream.foreach(Sales_Revenue_SummaryWriter).outputMode("append").start()
    }


    log.info("Reading tblADCadvertiser_rep_revenues from kafka streams")
    val tblADCadvertiser_rep_revenues = rawData.select($"topic", $"partition", $"offset", from_json($"value", TblADCadvertiser_rep_revenues.jsonSchema).as("value")).filter($"value.table" === "tblADCadvertiser_rep_revenues").select($"topic", $"partition", $"offset", $"value.type".as("opType")
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
        |      "adcentraldb" AS db
        |     ,"tblADCadvertiser_rep_revenues" AS tbl
        |     ,topic
        |     ,partition
        |     ,offset
        |     ,year
        |     ,quarter
        |     ,user_id
        |     ,0 AS sales_revenue
        |     ,CASE WHEN relationship = 'AGENCY_REP' THEN revenue ELSE 0 END AS agency_revenue
        |     ,CASE WHEN relationship = 'STRATEGIC_REP' THEN revenue ELSE 0 END AS strategic_revenue
        |     ,0 AS sales_new_revenue
        |     ,0 AS new_parent_revenue
        |FROM
        |(
        |SELECT
        |      topic
        |     ,partition
        |     ,offset
        |     ,YEAR(date) AS year
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
