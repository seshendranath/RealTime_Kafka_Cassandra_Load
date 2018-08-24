package com.indeed.dataengineering.task


/**
  * Created by aguyyala on 10/19/17.
  */


import org.apache.spark.sql._
import com.indeed.dataengineering.AnalyticsTaskApp._


class SalesSummaryInitial_Load {

  def run(): Unit = {
    import spark.implicits._

    val sql = spark.sql _

    log.info("Reading adcentraldb.tbladcquota from cassandra")
    val quotas = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcquota", "keyspace" -> "adcentraldb")).load.repartition(1)
    quotas.persist
    log.info(s"Count of adcentraldb.tbladcquota from cassandra: ${quotas.count}")
    quotas.createOrReplaceTempView("quotas")

    val quotas_by_user_quarter = sql("SELECT year, quarter(to_date(CAST(unix_timestamp(CAST(month AS STRING), 'M') AS TIMESTAMP))) AS quarter, user_id, SUM(quota) AS quota FROM quotas GROUP BY 1, 2, 3")

    log.info("Writing aggregated quotas by user to adcentraldb.sales_revenue_quota_summary_by_user_quarter in cassandra")
    quotas_by_user_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_user_quarter", "keyspace" -> "adcentraldb")).save


    val quotas_by_quarter = sql("SELECT year, quarter(to_date(CAST(unix_timestamp(CAST(month AS STRING), 'M') AS TIMESTAMP))) AS quarter, SUM(quota) AS quota FROM quotas GROUP BY 1, 2")

    log.info("Writing aggregated quotas by quarter to adcentraldb.sales_revenue_quota_summary_by_quarter in cassandra")
    quotas_by_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_quarter", "keyspace" -> "adcentraldb")).save


    log.info("Reading adsystemdb.testadvertiserids from cassandra")
    val staticTestAdvertiserIds = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "testadvertiserids", "keyspace" -> "adsystemdb")).load.repartition(1)
    staticTestAdvertiserIds.persist
    log.info(s"Count of adsystemdb.testadvertiserids from cassandra: ${staticTestAdvertiserIds.count}")
    staticTestAdvertiserIds.createOrReplaceTempView("staticTestAdvertiserIds")


    log.info("Reading adsystemdb.tbladscurrency_rates from cassandra")
    val statictblADScurrency_rates = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladscurrency_rates", "keyspace" -> "adsystemdb")).load.select("activity_date", "from_currency", "exchange_rate").distinct.repartition(1)
    statictblADScurrency_rates.persist
    log.info(s"Count of adsystemdb.tbladscurrency_rates from cassandra: ${statictblADScurrency_rates.count}")
    statictblADScurrency_rates.createOrReplaceTempView("statictblADScurrency_rates")


    log.info("Reading adcentraldb.tbladcaccounts_salesrep_commissions from cassandra")
    val tblADCaccounts_salesrep_commissionsCT = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcaccounts_salesrep_commissions", "keyspace" -> "adcentraldb")).load.select(
      $"date"
      , $"salesrep_id".as("user_id")
      , $"advertiser_id"
      , $"newrevenue_jobsearch_millicents"
      , $"newrevenue_dradis_lifetime_millicents"
      , $"newrevenue_dradis_recurring_millicents"
      , $"newrevenue_resume_millicents"
      , $"revenue_jobsearch_millicents"
      , $"revenue_resume_millicents"
      , $"revenue_dradis_lifetime_millicents"
      , $"revenue_dradis_recurring_millicents"
    ).repartition(conf.getOrElse("numPartitions", "160").toInt)

    tblADCaccounts_salesrep_commissionsCT.persist
    log.info(s"Count of adcentraldb.tbladcaccounts_salesrep_commissions from cassandra: ${tblADCaccounts_salesrep_commissionsCT.count}")
    tblADCaccounts_salesrep_commissionsCT.createOrReplaceTempView("tblADCaccounts_salesrep_commissionsCT")


    log.info("Reading adcentraldb.tblcrmgeneric_product_credit from cassandra")
    val tblCRMgeneric_product_creditCT = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tblcrmgeneric_product_credit", "keyspace" -> "adcentraldb")).load.select(
      $"activity_date".as("date")
      , $"user_id"
      , $"relationship"
      , $"currency"
      , $"rejected"
      , $"revenue_generic_product_local"
    ).repartition(conf.getOrElse("numPartitions", "160").toInt)

    tblCRMgeneric_product_creditCT.persist
    log.info(s"Count of adcentraldb.tblcrmgeneric_product_credit from cassandra: ${tblCRMgeneric_product_creditCT.count}")
    tblCRMgeneric_product_creditCT.createOrReplaceTempView("tblCRMgeneric_product_creditCT")


    log.info("Reading adcentraldb.tbladcadvertiser_rep_revenues from cassandra")
    val tblADCadvertiser_rep_revenuesCT = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tbladcadvertiser_rep_revenues", "keyspace" -> "adcentraldb")).load.select(
      $"activity_date".as("date")
      , $"user_id"
      , $"advertiser_id"
      , $"relationship"
      , $"revenue_jobsearch_millicents"
      , $"revenue_resume_millicents"
    ).repartition(conf.getOrElse("numPartitions", "160").toInt)

    tblADCadvertiser_rep_revenuesCT.persist
    log.info(s"Count of adcentraldb.tbladcadvertiser_rep_revenues from cassandra: ${tblADCadvertiser_rep_revenuesCT.count}")
    tblADCadvertiser_rep_revenuesCT.createOrReplaceTempView("tblADCadvertiser_rep_revenuesCT")


    var query =
      """
        |SELECT
        |      YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,user_id
        |     ,SUM(COALESCE(newrevenue_jobsearch_millicents, 0) +
        |      COALESCE(newrevenue_dradis_lifetime_millicents, 0) +
        |      COALESCE(newrevenue_dradis_recurring_millicents, 0) +
        |      COALESCE(newrevenue_resume_millicents, 0)) AS sales_new_revenue
        |     ,SUM(COALESCE(revenue_jobsearch_millicents, 0) +
        |      COALESCE(revenue_resume_millicents, 0) +
        |      COALESCE(revenue_dradis_lifetime_millicents, 0) +
        |      COALESCE(revenue_dradis_recurring_millicents, 0)) AS sales_revenue
        |FROM tblADCaccounts_salesrep_commissionsCT a
        |LEFT OUTER JOIN staticTestAdvertiserIds b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
        |GROUP BY 1, 2, 3
      """.stripMargin

    log.info(s"Running Query: $query")
    val tblADCaccounts_salesrep_commissionsCTsr = sql(query)
    tblADCaccounts_salesrep_commissionsCTsr.persist
    log.info(s"Count of tblADCaccounts_salesrep_commissionsCTsr: ${tblADCaccounts_salesrep_commissionsCTsr.count}")
    tblADCaccounts_salesrep_commissionsCTsr.createOrReplaceTempView("tblADCaccounts_salesrep_commissionsCTsr")


    query =
      """
        |SELECT
        |      YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,user_id
        |     ,SUM(CASE WHEN relationship IN ('SALES_REP', 'STRATEGIC_REP') THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END) AS sales_revenue
        |     ,SUM(CASE WHEN relationship = 'AGENCY_REP' THEN CAST(((total_local * exchange_rate) / 1000) AS BIGINT) ELSE 0 END) AS agency_revenue
        |FROM
        |(SELECT
        |      date
        |     ,user_id
        |     ,currency
        |     ,relationship
        |     ,COALESCE(revenue_generic_product_local, 0) AS total_local
        |FROM tblCRMgeneric_product_creditCT
        |WHERE relationship in ('SALES_REP', 'STRATEGIC_REP', 'AGENCY_REP') and rejected = 0
        |) arr
        |INNER JOIN (SELECT * FROM statictblADScurrency_rates) er
        |ON arr.date = er.activity_date AND arr.currency = er.from_currency
        |GROUP BY 1, 2, 3
      """.stripMargin

    log.info(s"Running Query: $query")
    val tblCRMgeneric_product_creditCTsr = sql(query)
    tblCRMgeneric_product_creditCTsr.persist
    log.info(s"Count of tblCRMgeneric_product_creditCTsr: ${tblCRMgeneric_product_creditCTsr.count}")
    tblCRMgeneric_product_creditCTsr.createOrReplaceTempView("tblCRMgeneric_product_creditCTsr")


    query =
      """
        |SELECT
        |      YEAR(date) AS year
        |     ,QUARTER(date) AS quarter
        |     ,user_id
        |     ,SUM(CASE WHEN relationship = 'AGENCY_REP' THEN COALESCE(revenue_jobsearch_millicents, 0) + COALESCE(revenue_resume_millicents, 0) ELSE 0 END) AS agency_revenue
        |     ,SUM(CASE WHEN relationship = 'STRATEGIC_REP' THEN COALESCE(revenue_jobsearch_millicents, 0) + COALESCE(revenue_resume_millicents, 0) ELSE 0 END) AS strategic_revenue
        |FROM tblADCadvertiser_rep_revenuesCT a
        |LEFT OUTER JOIN
        |staticTestAdvertiserIds b
        |ON a.advertiser_id = b.id
        |WHERE b.id IS NULL
        |AND relationship IN ('AGENCY_REP', 'STRATEGIC_REP')
        |GROUP BY 1, 2, 3
      """.stripMargin

    log.info(s"Running Query: $query")
    val tblADCadvertiser_rep_revenuesCTsr = sql(query)
    tblADCadvertiser_rep_revenuesCTsr.persist
    log.info(s"Count of tblADCadvertiser_rep_revenuesCTsr: ${tblADCadvertiser_rep_revenuesCTsr.count}")
    tblADCadvertiser_rep_revenuesCTsr.createOrReplaceTempView("tblADCadvertiser_rep_revenuesCTsr")


    query =
      """
        |SELECT
        |      year
        |     ,quarter
        |     ,user_id
        |     ,sales_revenue
        |     ,agency_revenue
        |     ,sales_new_revenue
        |     ,strategic_revenue
        |     ,sales_revenue + agency_revenue + sales_new_revenue + strategic_revenue AS total_revenue
        |FROM
        |(
        |SELECT
        |      COALESCE(a.year, b.year, c.year) AS year
        |     ,COALESCE(a.quarter, b.quarter, c.quarter) AS quarter
        |     ,COALESCE(a.user_id, b.user_id, c.user_id) AS user_id
        |     ,CAST(SUM(COALESCE(a.sales_revenue, 0) + COALESCE(b.sales_revenue, 0)) AS BIGINT) AS sales_revenue
        |     ,CAST(SUM(COALESCE(b.agency_revenue, 0) + COALESCE(c.agency_revenue, 0)) AS BIGINT) AS agency_revenue
        |     ,CAST(SUM(COALESCE(a.sales_new_revenue, 0)) AS BIGINT) AS sales_new_revenue
        |     ,CAST(SUM(COALESCE(c.strategic_revenue, 0)) AS BIGINT) AS strategic_revenue
        |FROM tblADCaccounts_salesrep_commissionsCTsr a
        |FULL OUTER JOIN
        |tblCRMgeneric_product_creditCTsr b ON a.year = b.year AND a.quarter = b.quarter AND a.user_id = b.user_id
        |FULL OUTER JOIN
        |tblADCadvertiser_rep_revenuesCTsr c ON a.year = c.year AND a.quarter = c.quarter AND a.user_id = c.user_id
        |GROUP BY 1, 2, 3
        |)
      """.stripMargin

    log.info(s"Running Query: $query")
    val sales_revenue_summary_by_user_quarter = sql(query)
    sales_revenue_summary_by_user_quarter.persist
    log.info(s"Count of sales_revenue_summary_by_user_quarter: ${sales_revenue_summary_by_user_quarter.count}")
    sales_revenue_summary_by_user_quarter.createOrReplaceTempView("sales_revenue_summary_by_user_quarter")

    log.info(s"Writing sales_revenue_summary_by_user_quarter to cassandra table sales_revenue_summary_by_user_quarter")
    sales_revenue_summary_by_user_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_summary_by_user_quarter", "keyspace" -> "adcentraldb")).save

    log.info(s"Writing sales_revenue_summary_by_user_quarter to cassandra table sales_revenue_quota_summary_by_user_quarter")
    sales_revenue_summary_by_user_quarter.select("year", "quarter", "user_id", "total_revenue").write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_user_quarter", "keyspace" -> "adcentraldb")).save


    query =
      """
        |SELECT
        |      year
        |     ,quarter
        |     ,SUM(sales_revenue) AS sales_revenue
        |     ,SUM(agency_revenue) AS agency_revenue
        |     ,SUM(sales_new_revenue) AS sales_new_revenue
        |     ,SUM(strategic_revenue) AS strategic_revenue
        |     ,SUM(total_revenue) AS total_revenue
        |FROM sales_revenue_summary_by_user_quarter
        |GROUP BY 1, 2
      """.stripMargin

    log.info(s"Running Query: $query")
    val sales_revenue_summary_by_quarter = sql(query)
    sales_revenue_summary_by_quarter.persist
    log.info(s"Count of sales_revenue_summary_by_quarter: ${sales_revenue_summary_by_quarter.count}")
    sales_revenue_summary_by_quarter.createOrReplaceTempView("sales_revenue_summary_by_quarter")

    log.info(s"Writing sales_revenue_summary_by_quarter to cassandra table sales_revenue_summary_by_quarter")
    sales_revenue_summary_by_quarter.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_summary_by_quarter", "keyspace" -> "adcentraldb")).save

    log.info(s"Writing sales_revenue_summary_by_quarter to cassandra table sales_revenue_quota_summary_by_quarter")
    sales_revenue_summary_by_quarter.select("year", "quarter", "total_revenue").write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "sales_revenue_quota_summary_by_quarter", "keyspace" -> "adcentraldb")).save

  }
}
