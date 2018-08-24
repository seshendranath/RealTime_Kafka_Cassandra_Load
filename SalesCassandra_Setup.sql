CREATE TABLE adsystemdb.testadvertiserids (id bigint PRIMARY KEY);

DROP TABLE adcentraldb.sales_revenue_summary_by_user_quarter;
DROP TABLE adcentraldb.sales_revenue_quota_summary_by_user_quarter;
DROP TABLE adcentraldb.sales_revenue_summary_by_quarter;
DROP TABLE adcentraldb.sales_revenue_quota_summary_by_quarter;

SELECT * FROM adcentraldb.sales_revenue_summary_by_user_quarter;
SELECT * FROM adcentraldb.sales_revenue_quota_summary_by_user_quarter;
SELECT * FROM adcentraldb.sales_revenue_summary_by_quarter;
SELECT * FROM adcentraldb.sales_revenue_quota_summary_by_quarter;

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