# Databricks notebook source
import os
from pyspark.sql import SparkSession
from task_2.utils.util import clean_column_names, add_metadata, create_table_if_not_exists, log_error


script_name = "consumer_bronze"  # Hardcode the script name without extension
spark = SparkSession.builder.appName("ConsumerBronzeLayer").getOrCreate()

s3_path = "s3://tendo-de-test/consumer.csv" # TODO: look for filename patterns for future loads
table_name = "tendo.bronze.consumer"
ddl_path = "ddl/create_bronze_tables.sql"

try:
    create_table_if_not_exists(spark, table_name, ddl_path)

    df = spark.read.option("header", "true").csv(s3_path)
    cleaned_df = clean_column_names(df)
    final_df = add_metadata(cleaned_df, s3_path)

    final_df.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"Data from {s3_path} ingested to Bronze successfully.")
except Exception as e:
    error_message = f"Error ingesting data from {s3_path} to Bronze: {e}"
    print(error_message)
    log_error(error_message, script_name=script_name, log_dir="/dbfs/logs/")
finally:
    spark.stop()

