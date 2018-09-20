/* 
36.7 MB -> 118.4 MB
1 MB -> 3.2261580381
106.7 GB -> 109,260.8 MB -> 352,492.608169237 MB -> 344.23 GB
*/

import org.apache.spark.sql._
import com.datastax.driver.core._

val cluster = Cluster.builder().addContactPoint("172.31.31.252").build()
val session = cluster.connect("adcentraldb")


val db = "adcentraldb"
val tbl = "tblADCsummary_advertiser"

spark.conf.set("spark.cassandra.connection.host", "172.31.31.252,172.31.22.160,172.31.26.117,172.31.19.127")

val df = spark.read.parquet("s3a://indeed-de/dsmith/output8/tbladcsummary_advertiser_history/201701/parquet/part-00000-fae227b4-4e6a-4e6d-86dc-239c9d0b9ccb-c000.snappy.parquet").select("advertiser_id","account_id","masteraccount_id","company","date_created","active","monthly_budget","monthly_budget_local","expended_budget","expended_budget_local","lifetime_budget_local","expended_lifetime_budget_local","type","payment_method","billing_threshold","billing_threshold_local","industry","is_ad_agency","agency_discount","process_level","estimated_budget","estimated_budget_local","first_revenue_date","last_revenue_date","account_email","billing_zip","billing_state","billing_country","entity","vat_country","vat_number","vat_exempt_13b_number","agency_rep","sales_rep","service_rep","strategic_rep","sj_count","hj_count","hjr_count","daily_budget_hits_days","daily_budget_hits_ads","predicted_spend","predicted_spend_local","showhostedjobs","bid_managed","companypagestatus","sponjobindeedapply","locale","last_note_date","last_email_date","last_call_date","last_meeting_date","last_proposal_date","last_activity_date","parent_company_id","currency","nps_likely","nps_notusing_reason","commission_start_date","commission_last_date","employee_count","timestamp_created","date_modified","is_deleted", "etl_file_inserted_timestamp")
df.persist
val insert_count = df.count
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
        |WHERE rank = 1
      """.stripMargin

val updatedDF = sql(query)
updatedDF.createOrReplaceTempView("updatedDF")
updatedDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> tbl.toLowerCase, "keyspace" -> db)).save


val latest_dt_modified = sql("SELECT MAX(binlog_timestamp) FROM updatedDF").collect.head.get(0).toString.take(19)

val cQuery = s"SELECT MIN(topic) AS topic, MIN(partition) AS partition, MIN(offset) AS offset FROM metadata.kafka_metadata WHERE db = '$db' AND tbl = '$tbl' AND tbl_date_modified = '$latest_dt_modified'"
val res = session.execute(cQuery).all.asScala.toArray.head
val (topic, partition, offset) = (res.getString("topic"), res.getInt("partition"), res.getLong("offset"))

val cQuery = s"INSERT INTO metadata.streaming_metadata (job, db, tbl, topic, partition, offset) VALUES('${table.capitalize + "_Load"}', '$db', '$table', '$topic', $partition, $offset)"
session.execute(cQuery)

val cQuery = s"UPDATE stats.streaming_stats SET inserted_records = inserted_records + $insert_count WHERE job = '${table.capitalize + "_Load"}' AND db = '$db' AND tbl = '$table'"
session.execute(cQuery)