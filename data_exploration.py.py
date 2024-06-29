# Databricks notebook source
from pyspark.sql import SparkSession
import re

# Load raw data from S3
avocado_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/avocado.csv")
consumer_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/consumer.csv")
fertilizer_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/fertilizer.csv")
purchase_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/purchase.csv")


# COMMAND ----------

avocado_df.printSchema()
consumer_df.printSchema()
fertilizer_df.printSchema()
purchase_df.printSchema()

# COMMAND ----------

# Count rows
print("Avocado rows:", avocado_df.count())
print("Consumer rows:", consumer_df.count())
print("Fertilizer rows:", fertilizer_df.count())
print("Purchase rows:", purchase_df.count())

# COMMAND ----------

# Check for null values
from pyspark.sql.functions import col, isnan, when, count

avocado_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in avocado_df.columns]).show()
consumer_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in consumer_df.columns]).show()
fertilizer_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in fertilizer_df.columns]).show()
purchase_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in purchase_df.columns]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC Initial observations:
# MAGIC - Some coumn names have spaces (needs to be standarized to snake case to load nicely into data warehouse)
# MAGIC - All columns are string type (will need to investigate what types to convert to)
# MAGIC - 

# COMMAND ----------

# MAGIC %sql
# MAGIC show external locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the catalog and bronze schema
# MAGIC use catalog tendo_de;
# MAGIC CREATE SCHEMA IF NOT EXISTS tendo_de.bronze;
# MAGIC
# MAGIC -- Define the bronze tables with STRING data types
# MAGIC CREATE TABLE IF NOT EXISTS tendo_de.bronze.purchase (
# MAGIC     consumerid STRING,
# MAGIC     purchaseid STRING,
# MAGIC     graphed_date STRING,
# MAGIC     avocado_bunch_id STRING,
# MAGIC     reporting_year STRING,
# MAGIC     QA_process STRING,
# MAGIC     billing_provider_sku STRING,
# MAGIC     grocery_store_id STRING,
# MAGIC     price_index STRING
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tendo_de.bronze.avocado (
# MAGIC     consumerid STRING,
# MAGIC     purchaseid STRING,
# MAGIC     avocado_bunch_id STRING,
# MAGIC     plu STRING,
# MAGIC     ripe_index_when_picked STRING,
# MAGIC     born_date STRING,
# MAGIC     picked_date STRING,
# MAGIC     sold_date STRING
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tendo_de.bronze.fertilizer (
# MAGIC     purchaseid STRING,
# MAGIC     consumerid STRING,
# MAGIC     fertilizerid STRING,
# MAGIC     type STRING,
# MAGIC     mg STRING,
# MAGIC     frequency STRING
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tendo_de.bronze.consumer (
# MAGIC     consumerid STRING,
# MAGIC     sex STRING,
# MAGIC     ethnicity STRING,
# MAGIC     race STRING,
# MAGIC     age STRING
# MAGIC );
# MAGIC

# COMMAND ----------


