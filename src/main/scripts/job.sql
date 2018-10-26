DROP TABLE IF EXISTS eravana.job;


CREATE TABLE IF NOT EXISTS eravana.job
(
   job_id SERIAL PRIMARY KEY
  ,job_name VARCHAR(100) NOT NULL
  ,process_name VARCHAR(100) NOT NULL
  ,object_name VARCHAR(100) NOT NULL
  ,job_type VARCHAR(100)
  ,job_group VARCHAR(100)
  ,job_frequency VARCHAR(100)
  ,description TEXT
  ,active_flag BOOLEAN
  ,date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  ,date_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  ,UNIQUE (job_name, process_name, object_name)
);


CREATE OR REPLACE FUNCTION update_date_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.date_modified = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


CREATE TRIGGER update_job_date_modified BEFORE UPDATE ON eravana.job FOR EACH ROW EXECUTE PROCEDURE update_date_modified_column();


CREATE TABLE IF NOT EXISTS eravana.job_instance
(
  job_id INTEGER REFERENCES eravana.job (job_id)
 ,instance_id UUID NOT NULL PRIMARY KEY
 ,instance_start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
 ,etl_start_time TIMESTAMP
 ,etl_end_time TIMESTAMP
 ,status_flag SMALLINT CHECK (status_flag IN (-1, 0, 1))
 ,instance_end_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
 ,date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
 ,date_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
