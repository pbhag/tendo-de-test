# Databricks notebook source
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.util import deduplicate_data, validate_and_enforce_schema, log_error

# Define the expected schema
schema = StructType([
    StructField("consumerid", LongType(), nullable=False),
    StructField("sex", StringType(), nullable=True),
    StructField("ethnicity", StringType(), nullable=True),
    StructField("race", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("raw_file_name", StringType(), nullable=True),
    StructField("load_timestamp", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True)  # Add updated_at column
])


bronze_table = "tendo.bronze.consumer"
silver_table = "tendo.silver.consumer"


# Read data from the Bronze layer
df = spark.read.format("delta").table(bronze_table)

# Add the current timestamp to the updated_at column
df = df.withColumn("updated_at", current_timestamp())

# Validate and enforce schema
df_validated = validate_and_enforce_schema(df, schema)

# Deduplicate data on primary key
df_deduped = deduplicate_data(df_validated, ["consumerid"])

# Data quality checks
df_clean = df_deduped.filter(
    col("consumerid").isNotNull()
)

# Ensure the schema matches before merging
df_clean = df_clean.select([col(field.name) for field in schema.fields])

# Check if the Silver table exists
if not spark.catalog.tableExists(silver_table):
    df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    # Merge into Silver table using Delta Lake's merge functionality
    delta_table = DeltaTable.forName(spark, silver_table)
    delta_table.alias("t") \
        .merge(
            df_clean.alias("s"),
            "t.consumerid = s.consumerid"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

print(f"Data from {bronze_table} upserted to Silver table: {silver_table}")


