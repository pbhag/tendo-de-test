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

# Summary statistics
print("avocado")
avocado_df.describe().show()
print("consumer")
consumer_df.describe().show()
print("fertilizer")
fertilizer_df.describe().show()
print("purchase")
purchase_df.describe().show()

# COMMAND ----------

# Check for duplicates
print("Avocado duplicates:", avocado_df.count() - avocado_df.dropDuplicates().count())
print("Consumer duplicates:", consumer_df.count() - consumer_df.dropDuplicates().count())
print("Fertilizer duplicates:", fertilizer_df.count() - fertilizer_df.dropDuplicates().count())
print("Purchase duplicates:", purchase_df.count() - purchase_df.dropDuplicates().count())

# COMMAND ----------

# MAGIC %md
# MAGIC Initial observations:
# MAGIC
# MAGIC - Some column names have spaces (needs to be standarized to snake case to load nicely into data warehouse):
# MAGIC   - `avocado.ripe index when picked` needs to be snake case
# MAGIC   - `purchase.QA process` needs to be snake case
# MAGIC
# MAGIC - All columns are string type (will need to investigate what types to convert to)
# MAGIC - Avocado 
# MAGIC   - has no null values
# MAGIC   - issues in data - min `picked_date` is before min `born_date` which doesn't make sense, data issue. Also data types dont match (date vs timestamp)
# MAGIC   - will need to implement logic to check that `picked_date` is after `born_date`, and that `sold_date` is also after `picked_date` to get valid data
# MAGIC - Consumer has a null for `age`, but that might not be an issue
# MAGIC   - `age` min is 190, max is 36 - data issue
# MAGIC - Fertilizer has a null value for `purchaseid` and `consumerid`, both of which are foriegn keys that are to be NOT NULL. No nulls for primary key `fertilizerid`
# MAGIC   - `mg` is mostly NULL and has tons of variance 
# MAGIC   - `frequency` is not standardized at all, need to understand how this is measured/recorded
# MAGIC   - several duplicates, half the data is duplicated 
# MAGIC - Purchase has a null value for primary key `purchaseid` and foreign key `consumerid` (which is also to be NOT NULL). Many other nulls in other columns

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

from pyspark.sql.functions import col

# Standardize column names
def standardize_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

avocado_df = standardize_column_names(avocado_df)
consumer_df = standardize_column_names(consumer_df)
fertilizer_df = standardize_column_names(fertilizer_df)
purchase_df = standardize_column_names(purchase_df)

# COMMAND ----------

# Write the Spark DataFrames to tables
avocado_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("tendo_de.bronze.avocado")
consumer_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("tendo_de.bronze.consumer")
fertilizer_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("tendo_de.bronze.fertilizer")
purchase_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("tendo_de.bronze.purchase")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tendo_de.bronze.avocado;
# MAGIC select * from tendo_de.bronze.consumer;

# COMMAND ----------


