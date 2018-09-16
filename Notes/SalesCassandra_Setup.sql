CREATE TABLE adsystemdb.testadvertiserids (id bigint PRIMARY KEY);

DROP TABLE adcentraldb.sales_revenue_summary_by_user_quarter;
DROP TABLE adcentraldb.sales_revenue_quota_summary_by_user_quarter;
DROP TABLE adcentraldb.sales_revenue_summary_by_quarter;
DROP TABLE adcentraldb.sales_revenue_quota_summary_by_quarter;

SELECT * FROM adcentraldb.sales_revenue_summary_by_user_quarter;
SELECT * FROM adcentraldb.sales_revenue_quota_summary_by_user_quarter;
SELECT * FROM adcentraldb.sales_revenue_summary_by_quarter;
SELECT * FROM adcentraldb.sales_revenue_quota_summary_by_quarter;

CREATE TABLE metadata.kafka_metadata
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

CREATE INDEX binlog_position_idx ON metadata.kafka_metadata ( binlog_position );

ALTER TABLE metadata.kafka_metadata WITH gc_grace_seconds = 600 AND
compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4', 'unchecked_tombstone_compaction': 'true',
'tombstone_compaction_interval': '600', 'tombstone_threshold': '0.1'};

CREATE TABLE metadata.streaming_metadata_extended
(
 job text,
 db text,
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
 PRIMARY KEY (job, db, tbl)
);

CREATE TABLE metadata.streaming_metadata
(
 job text,
 db text,
 tbl text,
 topic text,
 partition int,
 offset bigint,
 PRIMARY KEY (job, db, tbl)
);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblADCaccounts_salesrep_commissions_Load', 'adcentraldb', 'tblADCaccounts_salesrep_commissions', 'maxwell', 9, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblADCadvertiser_rep_revenues_Load', 'adcentraldb', 'tblADCadvertiser_rep_revenues', 'maxwell', 1, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('Tbladvertiser_Load', 'adsystemdb', 'tbladvertiser', 'maxwell', 9, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblADCparent_company_Load', 'adcentraldb', 'tblADCparent_company', 'maxwell', 4, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblADCparent_company_advertisers_Load', 'adcentraldb', 'tblADCparent_company_advertisers', 'maxwell', 3, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblCRMgeneric_product_credit_Load', 'adcentraldb', 'tblCRMgeneric_product_credit', 'maxwell', 7, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblADCquota_Load', 'adcentraldb', 'tblADCquota', 'maxwell', 4, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('TblADScurrency_rates_Load', 'adsystemdb', 'tblADScurrency_rates', 'maxwell', 7, -1);



INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adcentraldb', 'tblADCaccounts_salesrep_commissions', 'maxwell', 9, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adcentraldb', 'tblADCadvertiser_rep_revenues', 'maxwell', 1, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adsystemdb', 'tbladvertiser', 'maxwell', 9, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adcentraldb', 'tblADCparent_company', 'maxwell', 4, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adcentraldb', 'tblADCparent_company_advertisers', 'maxwell', 3, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adcentraldb', 'tblCRMgeneric_product_credit', 'maxwell', 7, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adcentraldb', 'tblADCquota', 'maxwell', 4, -1);

INSERT INTO streaming_metadata (job, db, tbl, topic, partition, offset) 
VALUES('KafkaMetadata_Load', 'adsystemdb', 'tblADScurrency_rates', 'maxwell', 7, -1);


CREATE TABLE stats.streaming_stats
(
 job text,
 db text,
 tbl text,
 inserted_records COUNTER,
 updated_records COUNTER,
 deleted_records COUNTER,
 total_records_processed COUNTER,
 PRIMARY KEY (job, db, tbl)
);

CREATE TABLE stats.streaming_stats_by_hour
(
 job text,
 db text,
 tbl text,
 dt Date,
 hr int,
 inserted_records COUNTER,
 updated_records COUNTER,
 deleted_records COUNTER,
 total_records_processed COUNTER,
 PRIMARY KEY (job, db, tbl, dt, hr)
);

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