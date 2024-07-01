USE DATABASE tendo;
CREATE SCHEMA IF NOT EXISTS tendo.gold;

CREATE TABLE IF NOT EXISTS tendo.gold.output (
  consumerid LONG,
  sex STRING,
  age INT,
  avocado_days_sold INT,
  avocado_ripe_index INT,
  avocado_days_picked INT,
  fertilizer_type STRING,
  updated_at TIMESTAMP
) USING DELTA;