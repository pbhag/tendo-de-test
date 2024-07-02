# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp
from task_2.utils.load_utils import create_table_if_not_exists


file_path = "s3://tendo-de-test/purchase" 
table_name = "tendo.bronze.purchase"
ddl_path = "ddl/create_bronze_tables.sql"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


# create_table_if_not_exists(spark, table_name, ddl_path)

df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file_path)
  .select("purchaseid",
          "consumerid"
          "graphed_date",
          "avocado_bunch_id", 
          "reporting_year",
          col("QA process").alias("qa_process"),
          "billing_provider_sku",
          "picked_date",
          "grocery_store_id",
          "price_index",
          col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))

