USE DATABASE tendo;
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS tendo.bronze.purchase (
  purchaseid STRING,
  consumerid STRING,
  graphed_date STRING,
  avocado_bunch_id STRING,
  reporting_year STRING,
  qa_process STRING,
  billing_provider_sku STRING,
  grocery_store_id STRING,
  price_index STRING,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS tendo.bronze.avocado (
  consumerid STRING,
  purchaseid STRING,
  avocado_bunch_id STRING,
  plu STRING,
  ripe_index_when_picked STRING,
  born_date STRING,
  picked_date STRING,
  sold_date STRING,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS tendo.bronze.fertilizer (
  purchaseid STRING,
  consumerid STRING,
  fertilizerid STRING,
  type STRING,
  mg STRING,
  frequency STRING,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS tendo.bronze.consumer (
  consumerid STRING,
  sex STRING,
  ethnicity STRING,
  race STRING,
  age STRING,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;
