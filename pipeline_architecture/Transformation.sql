USE DATABASE ECOMMERCE_DB;
USE SCHEMA PIPELINE_SCHEMA;
-- 1. Create a Stream to track changes (Change Data Capture)
CREATE STREAM raw_transactions_stream ON TABLE raw_transactions;

-- 2. Create the Curated/Reporting Table
CREATE TABLE daily_sales_summary (
    sales_date DATE,
    total_revenue NUMBER(12,2),
    total_transactions INT
);ECOMMERCE_DB.PIPELINE_SCHEMA.RAW_TRANSACTIONS

-- 3. Create a Task to aggregate data from the stream hourly
CREATE TASK aggregate_sales_task
  WAREHOUSE = compute_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_transactions_stream')
AS
  INSERT INTO daily_sales_summary (sales_date, total_revenue, total_transactions)
  SELECT 
      DATE(transaction_date),
      SUM(amount),
      COUNT(transaction_id)
  FROM raw_transactions_stream
  GROUP BY DATE(transaction_date);

-- 4. Turn the task on (Tasks are created in a suspended state)
ALTER TASK aggregate_sales_task RESUME;

EXECUTE TASK aggregate_sales_task;
