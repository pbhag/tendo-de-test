# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

file_path = "s3://tendo-de-test/avocado" # TODO: look for filename patterns for future loads

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"tendo.bronze.avocado"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file_path)
  .select("consumerid",
          "purchaseid",
          "avocado_bunch_id",
          "plu", 
          col("ripe index when picked").alias("ripe_index_when_picked"),
          "born_date",
          "picked_date",
          "sold_date",
          col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .toTable(table_name))

# COMMAND ----------


