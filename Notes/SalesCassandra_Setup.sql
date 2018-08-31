CREATE TABLE adsystemdb.testadvertiserids (id bigint PRIMARY KEY);

DROP TABLE adcentraldb.sales_revenue_summary_by_user_quarter;
DROP TABLE adcentraldb.sales_revenue_quota_summary_by_user_quarter;
DROP TABLE adcentraldb.sales_revenue_summary_by_quarter;
DROP TABLE adcentraldb.sales_revenue_quota_summary_by_quarter;

SELECT * FROM adcentraldb.sales_revenue_summary_by_user_quarter;
SELECT * FROM adcentraldb.sales_revenue_quota_summary_by_user_quarter;
SELECT * FROM adcentraldb.sales_revenue_summary_by_quarter;
SELECT * FROM adcentraldb.sales_revenue_quota_summary_by_quarter;

CREATE TABLE kafka_metadata
(db text,
 tbl text,
 tbl_date_modified timestamp,
 topic text,
 partition int,
 offset bigint,
 primary_key text,
 binlog_position text,
 kafka_timestamp timestamp,
 binlog_timestamp timestamp,
 meta_last_updated timestamp,
 PRIMARY KEY ((db, tbl, tbl_date_modified), topic, partition, offset)
) WITH default_time_to_live = 864000;

CREATE INDEX binlog_position_idx ON kafka_metadata ( binlog_position );


CREATE TABLE adcentraldb.sales_revenue_summary_by_user_quarter
(
	year Int,
	quarter Int,
	user_id BigInt,
	sales_revenue COUNTER,
	agency_revenue COUNTER,
	strategic_revenue COUNTER,
	sales_new_revenue COUNTER,
	total_revenue COUNTER,
	PRIMARY KEY(year, quarter, user_id)
);

CREATE TABLE adcentraldb.sales_revenue_quota_summary_by_user_quarter
(
	year Int,
	quarter Int,
	user_id BigInt,
	quota Decimal,
	total_revenue BigInt,
	PRIMARY KEY(year, quarter, user_id)
);


CREATE TABLE adcentraldb.sales_revenue_summary_by_quarter
(
	year Int,
	quarter Int,
	sales_revenue COUNTER,
	agency_revenue COUNTER,
	strategic_revenue COUNTER,
	sales_new_revenue COUNTER,
	total_revenue COUNTER,
	PRIMARY KEY(year, quarter)
);


CREATE TABLE adcentraldb.sales_revenue_quota_summary_by_quarter
(
	year Int,
	quarter Int,
	quota Decimal,
	total_revenue BigInt,
	PRIMARY KEY(year, quarter)
);