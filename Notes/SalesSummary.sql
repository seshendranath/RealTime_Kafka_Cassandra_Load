Tables:
 AdSystem:
    tblADScurrency_rates
    tbladvertiser
 AdCentral:
    tblADCquota
    tblADCsummary_sales_dashboard // Optional
    tblADCaccounts_salesrep_commissions
    tblCRMgeneric_product_credit
    tblADCadvertiser_rep_revenues
    tblADCparent_company
    tblADCparent_company_advertisers
    tblACLusers


Table Count:
-- AdSystem
SELECT COUNT(*) FROM tbladvertiser; -- Record Count: 22M PK: id
SELECT COUNT(*) FROM tblADScurrency_rates; -- Record Count: 19K PK: activity_date,to_currency,from_currency

-- AdCentral
SELECT COUNT(*) FROM tblADCaccounts_salesrep_commissions; -- Record Count: 202M PK: date,advertiser_id
SELECT COUNT(*) FROM tblADCadvertiser_rep_revenues; -- Record Count: 190M PK: activity_date,advertiser_id,relationship
SELECT COUNT(*) FROM tblADCparent_company_advertisers; -- Record Count: 13M PK: advertiser_id,parent_company_id
SELECT COUNT(*) FROM tblADCparent_company; -- Record Count: 10M PK: id
SELECT COUNT(*) FROM tblCRMgeneric_product_credit; -- Record Count: 206K PK: id
SELECT COUNT(*) FROM tblADCquota; -- Record Count: 42K PK: year,month,user_id,quota_type
SELECT COUNT(*) FROM tblACLusers; -- Record Count: 12K PK: id



val currencies = SELECT DISTINCT from_currency FROM adsystemdb.tblADScurrency_rates;
val testAdvertiserIds: List[Int] = SELECT id FROM adsystemdb.tbladvertiser WHERE type="Test";

for each quarter
val rates: Table<Date, Currency, Integer> =
                SELECT activity_date, from_currency, exchange_rate
                FROM tblADScurrency_rates
                WHERE activity_date BETWEEN '2018-04-01' AND '2018-06-30'
                ORDER BY activity_date

// quotaMillicents is USD Milli cents. Applied rate to convert quota_local to USD
val quotas: Table<Integer, String, Quota> = 
        SELECT user_id, quota_type,quota_local, (quota_local * rate)/1000 AS quotaMillicents, q.currency FROM
        (SELECT user_id, quota_type, quota_local, currency
        FROM adcentraldb.tblADCquota
        WHERE year=2018
        AND month=4
        AND quota_local > 0) AS q
        JOIN 
        // (SELECT from_currency AS currency, exchange_rate AS rate FROM rates WHERE activity_date = '{first_day_of_quarter}') r
        (SELECT * FROM (
        SELECT 'AED' AS currency, 272300 AS rate
        UNION ALL
        SELECT 'AUD' AS currency, 768970 AS rate
        UNION ALL
        SELECT 'BRL' AS currency, 302870 AS rate
        UNION ALL
        SELECT 'CAD' AS currency, 776040 AS rate
        UNION ALL
        SELECT 'CHF' AS currency, 1048660 AS rate
        UNION ALL
        SELECT 'EUR' AS currency, 1232170 AS rate
        UNION ALL
        SELECT 'GBP' AS currency, 1403740 AS rate
        UNION ALL
        SELECT 'INR' AS currency, 15400 AS rate
        UNION ALL
        SELECT 'JPY' AS currency, 941000 AS rate
        UNION ALL
        SELECT 'SGD' AS currency, 763110 AS rate
        UNION ALL
        SELECT 'USD' AS currency, 1000000 AS rate
        ) tmp) AS r
        ON q.currency = r.currency


// START of SALES_NEW_REVENUE
val distinctUserIds = SELECT DISTINCT user_id FROM tblADCsummary_sales_dashboard WHERE year=2018 AND month=4 AND type='SALES_NEW_REVENUE'

// They used createCallbackHandler logic to convert the revenue_millicents to other local currencies using exchange_rates from rates.
// Ignore the logic for now and concentrate on just revenue_millicents which I assume is in USD and group by salesrep_id
val salesRepNewRevenue: Map<Integer, revenue_millicents>= 
        SELECT salesrep_id,
        (SUM(newrevenue_jobsearch_millicents) + 
               SUM(newrevenue_dradis_lifetime_millicents) + 
               SUM(newrevenue_dradis_recurring_millicents) + 
               SUM(newrevenue_resume_millicents)) AS revenue_millicents // revenue in USD milli cents to get actual USD just divide this col / 1000
        FROM tblADCaccounts_salesrep_commissions 
        WHERE date BETWEEN '2018-04-01' AND '2018-06-30'
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) // test advertiser_id
        GROUP BY salesrep_id
        HAVING revenue_millicents > 0

// Year = 2018, month = 4, type = SALES_NEW_REVENUE, 
// Applied Quota 
val salesRepNewRevenue = 
        SELECT 2018 AS YEAR, 4 AS MONTH, 'SALES_NEW_REVENUE' AS type,
        COALESCE(salesrep_id, user_id), COALESCE(revenue_millicents, 0), COALESCE(quota_local, 0)
          COALESCE(quotaMillicents, 0), COALESCE(currency, 'USD')
        FROM salesRepNewRevenue s FULL OUTER JOIN quotas q on s.salesrep_id = q.user_id


//START OF SALES_REVENUE
val distinctUserIds = SELECT DISTINCT user_id FROM tblADCsummary_sales_dashboard WHERE year=2018 AND month=4 AND type='SALES_REVENUE'

// They used createCallbackHandler logic to convert the revenue_millicents to other local currencies using exchange_rates from rates.
// Ignore the logic for now and concentrate on just revenue_millicents which I assume is in USD and group by salesrep_id
val salesRepRevenue: Map<Integer, revenue_millicents>= 
        SELECT salesrep_id,
        (SUM(revenue_jobsearch_millicents) + 
               SUM(revenue_resume_millicents) + 
               SUM(revenue_dradis_lifetime_millicents) + 
               SUM(revenue_dradis_recurring_millicents)) AS revenue_millicents 
        FROM tblADCaccounts_salesrep_commissions 
        WHERE date BETWEEN '2018-04-01' AND '2018-06-30'
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) // test advertiser_id
        GROUP BY salesrep_id 
        HAVING revenue_millicents>0


// genericSalesRepRevenue
val genericSalesRepRevenue = 
        SELECT USER_ID, SUM((TOTAL_LOCAL * exchange_rate) / 1000) AS revenue_millicents
        (SELECT USER_ID, ACTIVITY_DATE, sum(REVENUE_GENERIC_PRODUCT_LOCAL) as TOTAL_LOCAL, CURRENCY
        FROM tblCRMgeneric_product_credit
        WHERE ACTIVITY_DATE BETWEEN '2018-04-01' AND '2018-06-30'
        and RELATIONSHIP in ('SALES_REP', 'STRATEGIC_REP')
        and REJECTED = 0
        GROUP BY USER_ID, ACTIVITY_DATE, CURRENCY) a 
        INNER JOIN rates r
        ON a.ACTIVITY_DATE = r.activity_date AND a.CURRENCY = r.from_currency
        GROUP BY USER_ID

// Merge Revenues of salesRepRevenue and genericSalesRepRevenue
val salesRepRevenue = SELECT COALESCE(salesrep_id, USER_ID) AS user_id,  COALESCE(a.revenue_millicents, 0) + COALESCE(b.revenue_millicents, 0) AS revenue_millicents
FROM salesRepRevenue a FULL OUTER JOIN genericSalesRepRevenue
ON a.salesrep_id = b.USER_ID

// Year = 2018, month = 4, type = SALES_REVENUE, 
// Applied Quota 
val salesRepRevenue = 
        SELECT 2018 AS YEAR, 4 AS MONTH, 'SALES_REVENUE' AS type,
        COALESCE(salesrep_id, user_id), COALESCE(revenue_millicents, 0), COALESCE(quota_local, 0)
          COALESCE(quotaMillicents, 0), COALESCE(currency, 'USD')
        FROM salesRepRevenue s FULL OUTER JOIN quotas q on s.salesrep_id = q.user_id



// Agency Revenue
val distinctUserIds = SELECT DISTINCT user_id FROM tblADCsummary_sales_dashboard WHERE year=2018 AND month=4 AND type='AGENCY_REVENUE'

val primaryAgencyRepRevenue = 
        SELECT user_id,
        (SUM(revenue_jobsearch_millicents)+SUM(revenue_resume_millicents)) AS revenue_millicents
        FROM tblADCadvertiser_rep_revenues 
        WHERE activity_date BETWEEN '2018-04-01' AND '2018-06-30'
        AND relationship='AGENCY_REP'
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) -- test advertiser_id
        GROUP BY user_id
        HAVING revenue_millicents>0

val genericAgencyRepRevenue = 
        SELECT USER_ID, SUM((TOTAL_LOCAL * exchange_rate) / 1000) AS revenue_millicents
        (SELECT USER_ID, ACTIVITY_DATE, sum(REVENUE_GENERIC_PRODUCT_LOCAL) as TOTAL_LOCAL, CURRENCY
        FROM tblCRMgeneric_product_credit
        WHERE ACTIVITY_DATE BETWEEN '2018-04-01' AND '2018-06-30'
        and RELATIONSHIP in ('AGENCY_REP')
        and REJECTED = 0
        GROUP BY USER_ID, ACTIVITY_DATE, CURRENCY) a 
        INNER JOIN rates r
        ON a.ACTIVITY_DATE = r.activity_date AND a.CURRENCY = r.from_currency
        GROUP BY USER_ID

// Merge Revenues of primaryAgencyRepRevenue and genericSalesRepRevenue
val primaryAgencyRepRevenue = 
        SELECT COALESCE(a.user_id, b.USER_ID) AS user_id,  COALESCE(a.revenue_millicents, 0) + COALESCE(b.revenue_millicents, 0) AS revenue_millicents
        FROM primaryAgencyRepRevenue a FULL OUTER JOIN genericAgencyRepRevenue b
        ON a.user_id = b.USER_ID

// Year = 2018, month = 4, type = SALES_REVENUE, 
// Applied Quota 
val primaryAgencyRepRevenue = 
       SELECT 2018 AS YEAR, 4 AS MONTH, 'AGENCY_REVENUE' AS type,
       COALESCE(s.user_id, q.user_id), COALESCE(revenue_millicents, 0), COALESCE(quota_local, 0)
         COALESCE(quotaMillicents, 0), COALESCE(currency, 'USD')
       FROM primaryAgencyRepRevenue s FULL OUTER JOIN quotas q on s.user_id = q.user_id


// STRATEGIC Revenue

val distinctUserIds = SELECT DISTINCT user_id FROM tblADCsummary_sales_dashboard WHERE year=2018 AND month=4 AND type='STRATEGIC_REVENUE'

val strategicRepRevenue = 
       SELECT user_id,
       (SUM(revenue_jobsearch_millicents)+SUM(revenue_resume_millicents)) AS revenue_millicents
       FROM tblADCadvertiser_rep_revenues 
       WHERE activity_date BETWEEN '2018-04-01' AND '2018-06-30'
       AND relationship='STRATEGIC_REP'
       AND advertiser_id NOT IN (1,3,42,93,161,167,375) -- test advertiser_id
       GROUP BY user_id
       HAVING revenue_millicents>0

// Year = 2018, month = 4, type = SALES_REVENUE, 
// Applied Quota 
val strategicRepRevenue = 
       SELECT 2018 AS YEAR, 4 AS MONTH, 'STRATEGIC_REVENUE' AS type,
       COALESCE(s.user_id, q.user_id), COALESCE(revenue_millicents, 0), COALESCE(quota_local, 0)
         COALESCE(quotaMillicents, 0), COALESCE(currency, 'USD')
       FROM strategicRepRevenue s FULL OUTER JOIN quotas q on s.user_id = q.user_id




// NEW PARENT REVENUE

val distinctUserIds = select ID from tblACLusers where ENTITY != 'US' and WORKDAY_TEAM like '%NAM%';

val salesRepNewParentRevenue = 
       SELECT pc.user_id AS user_id
       (SUM(revenue_jobsearch_millicents) + 
       SUM(revenue_resume_millicents) + 
       SUM(revenue_dradis_lifetime_millicents) + 
       SUM(revenue_dradis_recurring_millicents)) AS revenue_millicents 
       FROM tblADCparent_company pc 
       JOIN tblADCparent_company_advertisers pca ON pc.id = pca.parent_company_id 
       JOIN tblADCaccounts_salesrep_commissions accsc ON accsc.advertiser_id = pca.advertiser_id 
       AND accsc.salesrep_id = pc.user_id 
       WHERE accsc.date >= '2018-04-01'
       AND accsc.date <= '2018-06-30'
       AND accsc.date <= DATE_ADD(pc.first_revenue_date, INTERVAL 89 DAY) 
       AND pc.first_revenue_date >= '2018-01-01'
       AND pc.user_id IN (select ID from tblACLusers where ENTITY != 'US' and WORKDAY_TEAM like '%NAM%')
       GROUP BY pc.user_id
       HAVING revenue_millicents > 0


val genericProductRevenue = 
       SELECT USER_ID, SUM((TOTAL_LOCAL * exchange_rate) / 1000) AS revenue_millicents
       (SELECT pc.USER_ID, tpc.ACTIVITY_DATE ,sum(tpc.REVENUE_GENERIC_PRODUCT_LOCAL) as TOTAL_LOCAL ,tpc.CURRENCY
       FROM tblADCparent_company pc
       JOIN tblADCparent_company_advertisers pca ON pca.PARENT_COMPANY_ID = pc.ID
       JOIN tblCRMgeneric_product_credit tpc ON tpc.ADVERTISER_ID = pca.ADVERTISER_ID
       AND tpc.USER_ID = pc.USER_ID
       WHERE tpc.REJECTED = 0
       AND pc.USER_ID IN (select ID from tblACLusers where ENTITY != 'US' and WORKDAY_TEAM like '%NAM%')
       AND tpc.RELATIONSHIP IN ('SALES_REP', 'STRATEGIC_REP')
       AND tpc.ACTIVITY_DATE >= '2018-04-01'
       AND tpc.ACTIVITY_DATE <= '2018-06-30'
       AND tpc.ACTIVITY_DATE <= DATE_ADD(pc.first_revenue_date, INTERVAL 89 DAY)
       AND pc.FIRST_REVENUE_DATE >= '2018-01-01'
       GROUP BY pc.USER_ID, tpc.ACTIVITY_DATE, tpc.CURRENCY) a 
       INNER JOIN rates r
       ON a.ACTIVITY_DATE = r.activity_date AND a.CURRENCY = r.from_currency
       GROUP BY USER_ID



// Merge Revenues of salesRepNewParentRevenue and genericSalesRepRevenue
val salesRepNewParentRevenue = 
       SELECT COALESCE(a.user_id, b.USER_ID) AS user_id,  COALESCE(a.revenue_millicents, 0) + COALESCE(b.revenue_millicents, 0) AS revenue_millicents
       FROM salesRepNewParentRevenue a FULL OUTER JOIN genericProductRevenue b
       ON a.user_id = b.USER_ID

// Year = 2018, month = 4, type = SALES_REVENUE, 
// Applied Quota 
val salesRepNewParentRevenue = 
       SELECT 2018 AS YEAR, 4 AS MONTH, 'NEW_PARENT_REVENUE' AS type,
       COALESCE(s.user_id, q.user_id), COALESCE(revenue_millicents, 0), COALESCE(quota_local, 0)
         COALESCE(quotaMillicents, 0), COALESCE(currency, 'USD')
       FROM salesRepNewParentRevenue s FULL OUTER JOIN quotas q on s.user_id = q.user_id



// COMBINE ALL REVENUES TOGETHER
val salesRevenue = 
        SELECT salesrep_id,
        (SUM(newrevenue_jobsearch_millicents) + 
               SUM(newrevenue_dradis_lifetime_millicents) + 
               SUM(newrevenue_dradis_recurring_millicents) + 
               SUM(newrevenue_resume_millicents)) AS SALES_NEW_REVENUE_revenue_millicents // revenue in USD milli cents to get actual USD just divide this col / 1000
        (SUM(revenue_jobsearch_millicents) + 
                 SUM(revenue_resume_millicents) + 
                 SUM(revenue_dradis_lifetime_millicents) + 
                 SUM(revenue_dradis_recurring_millicents)) AS SALES_REVENUE_revenue_millicents 
          FROM tblADCaccounts_salesrep_commissions 
          WHERE date BETWEEN '2018-04-01' AND '2018-06-30'
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) // test advertiser_id
        GROUP BY salesrep_id
        HAVING revenue_millicents > 0


// genericSalesRepRevenue
val genericSalesRepRevenue = 
        SELECT USER_ID, 
        CASE WHEN RELATIONSHIP in ('SALES_REP', 'STRATEGIC_REP') THEN SUM((TOTAL_LOCAL * exchange_rate) / 1000) ELSE 0 END AS genericSalesRepRevenue_revenue_millicents
        ,CASE WHEN RELATIONSHIP = 'AGENCY_REP' THEN SUM((TOTAL_LOCAL * exchange_rate) / 1000) ELSE 0 END AS genericAgencyRepRevenue_revenue_millicents
        (SELECT USER_ID, ACTIVITY_DATE, sum(REVENUE_GENERIC_PRODUCT_LOCAL) as TOTAL_LOCAL, CURRENCY
        FROM tblCRMgeneric_product_credit
        WHERE ACTIVITY_DATE BETWEEN '2018-04-01' AND '2018-06-30'
        and RELATIONSHIP in ('SALES_REP', 'STRATEGIC_REP', 'AGENCY_REP')
        and REJECTED = 0
        GROUP BY USER_ID, ACTIVITY_DATE, CURRENCY) a 
        INNER JOIN rates r
        ON a.ACTIVITY_DATE = r.activity_date AND a.CURRENCY = r.from_currency
        GROUP BY USER_ID


val primaryAgencyRepRevenue = 
        SELECT user_id,
        CASE WHEN relationship = 'AGENCY_REP' THEN (SUM(revenue_jobsearch_millicents)+SUM(revenue_resume_millicents)) ELSE 0 END AS primaryAgencyRepRevenue_revenue_millicents
        ,CASE WHEN relationship = 'STRATEGIC_REP' THEN (SUM(revenue_jobsearch_millicents)+SUM(revenue_resume_millicents)) ELSE 0 END AS strategicRepRevenue_revenue_millicents
        FROM tblADCadvertiser_rep_revenues 
        WHERE activity_date BETWEEN '2018-04-01' AND '2018-06-30'
        AND relationship IN ('AGENCY_REP', 'STRATEGIC_REP')
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) -- test advertiser_id
        GROUP BY user_id
        HAVING revenue_millicents>0


// Remove New Parent Revenue for now.


// Combine Further.
val salesRevenue = 
        SELECT salesrep_id AS user_id,
            (SUM(newrevenue_jobsearch_millicents) + 
             SUM(newrevenue_dradis_lifetime_millicents) + 
             SUM(newrevenue_dradis_recurring_millicents) + 
             SUM(newrevenue_resume_millicents) +
             SUM(revenue_jobsearch_millicents) + 
             SUM(revenue_resume_millicents) + 
             SUM(revenue_dradis_lifetime_millicents) + 
             SUM(revenue_dradis_recurring_millicents)) AS revenue_millicents 
        FROM tblADCaccounts_salesrep_commissions 
        WHERE date BETWEEN '2018-07-01' AND '2018-08-30'
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) // test advertiser_id
        GROUP BY salesrep_id
        HAVING revenue_millicents > 0


// genericSalesRepRevenue
val genericSalesRepRevenue = 
        SELECT USER_ID, SUM((TOTAL_LOCAL * exchange_rate) / 1000) AS revenue_millicents
        (SELECT USER_ID, ACTIVITY_DATE, sum(REVENUE_GENERIC_PRODUCT_LOCAL) as TOTAL_LOCAL, CURRENCY
        FROM tblCRMgeneric_product_credit
        WHERE ACTIVITY_DATE BETWEEN '2018-04-01' AND '2018-06-30'
        and RELATIONSHIP in ('SALES_REP', 'STRATEGIC_REP', 'AGENCY_REP')
        and REJECTED = 0
        GROUP BY USER_ID, ACTIVITY_DATE, CURRENCY) a 
        INNER JOIN rates r
        ON a.ACTIVITY_DATE = r.activity_date AND a.CURRENCY = r.from_currency
        GROUP BY USER_ID


val primaryAgencyRepRevenue = 
        SELECT user_id,
        SUM(revenue_jobsearch_millicents)+SUM(revenue_resume_millicents) AS revenue_millicents
        FROM tblADCadvertiser_rep_revenues 
        WHERE activity_date BETWEEN '2018-04-01' AND '2018-06-30'
        AND relationship IN ('AGENCY_REP', 'STRATEGIC_REP')
        AND advertiser_id NOT IN (1,3,42,93,161,167,375) -- test advertiser_id
        GROUP BY user_id
        HAVING revenue_millicents>0

