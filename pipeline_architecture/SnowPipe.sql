USE DATABASE ECOMMERCE_DB;
USE SCHEMA PIPELINE_SCHEMA;
-- 1. Create the Raw Landing Table
CREATE TABLE raw_transactions (
    transaction_id STRING,
    user_id STRING,
    product_id STRING,
    amount NUMBER(10,2),
    transaction_date TIMESTAMP
);

-- 2. Create the Snowpipe
CREATE PIPE transaction_pipe AUTO_INGEST = TRUE AS
  COPY INTO raw_transactions
  FROM @my_s3_stage;

ALTER PIPE transaction_pipe REFRESH;
