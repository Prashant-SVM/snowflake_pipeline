-- 1. Create the environment
CREATE WAREHOUSE IF NOT EXISTS compute_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60;
CREATE DATABASE ecommerce_db;
CREATE SCHEMA pipeline_schema;

-- 2. Create the Storage Integration (Connects Snowflake to AWS)
-- Note: You will need to configure an AWS IAM Role to get the ARN
CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::545009855145:role/Snowflake_list_read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-snowflake-ecommerce-raw/');

-- 3. Create the External Stage
CREATE STAGE my_s3_stage
  URL = 's3://my-snowflake-ecommerce-raw/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

