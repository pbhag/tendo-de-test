USE DATABASE tendo;
CREATE SCHEMA IF NOT EXISTS tendo.silver;

CREATE TABLE IF NOT EXISTS tendo.silver.purchase (
  purchaseid LONG,
  consumerid LONG,
  graphed_date DATE,
  avocado_bunch_id INT,
  reporting_year INT,
  qa_process STRING,
  billing_provider_sku LONG,
  grocery_store_id INT,
  price_index INT,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS tendo.silver.avocado (
  consumerid LONG,
  purchaseid LONG,
  avocado_bunch_id INT,
  plu LONG,
  ripe_index_when_picked INT,
  born_date DATE,
  picked_date DATE,
  sold_date DATE,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS tendo.silver.fertilizer (
  purchaseid LONG,
  consumerid LONG,
  fertilizerid LONG,
  type STRING,
  mg INT,
  frequency STRING,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS tendo.silver.consumer (
  consumerid LONG,
  sex STRING,
  ethnicity STRING,
  race STRING,
  age INT,
  raw_file_name STRING,
  load_timestamp TIMESTAMP
) USING DELTA;
