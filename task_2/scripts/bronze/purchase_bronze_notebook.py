# Databricks notebook source
import sys 
import os
from pyspark.sql import col, current_timestamp
from task_2.utils.util import clean_column_names, add_metadata, create_table_if_not_exists, log_error


file_path = "s3://tendo-de-test/purchase.csv" # TODO: look for filename patterns for future loads
table_name = "tendo.bronze.purchase"
ddl_path = "ddl/create_bronze_tables.sql"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

checkpoint_path = "s3://tendo-de-test/auto_loader/_checkpoint/avocado_bronze"





