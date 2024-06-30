# Databricks notebook source
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.util import deduplicate_data, enforce_schema, log_error

# Define the expected schema
avocado_schema = StructType([
    StructField("purchaseid", LongType(), False),
    StructField("consumerid", LongType(), False),
    StructField("avocado_bunch_id", IntegerType(), True),
    StructField("plu", LongType(), True),
    StructField("ripe_index_when_picked", IntegerType(), True),
    StructField("born_date", DateType(), True),
    StructField("picked_date", DateType(), True),
    StructField("sold_date", DateType(), True),
    StructField("raw_file_name", StringType(), True),
    StructField("load_timestamp", TimestampType(), True)
])




# # Merge into Silver table using Delta Lake's merge functionality

        # else:
        #     df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    #     print(f"Data from {bronze_table} upserted to Silver successfully.")
    # except Exception as e:
    #     error_message = f"Error upserting data from {bronze_table} to Silver: {e}"
    #     print(error_message)
    #     log_error(error_message, script_name=script_name, log_dir="/dbfs/logs/")
    # finally:
    #     spark.stop()

# main()


# COMMAND ----------

# def main():
script_name = "avocado_silver"
# spark = SparkSession.builder.appName("AvocadoSilverLayer").getOrCreate()

bronze_table = "tendo.bronze.avocado"
silver_table = "tendo.silver.avocado"

    # try:
        # Read data from the Bronze layer
df = spark.read.format("delta").table(bronze_table)

# COMMAND ----------

# Deduplicate data
df_deduped = deduplicate_data(df, ["consumerid"])

# Enforce schema
df_enforced = enforce_schema(df_deduped, avocado_schema)

# Data quality checks
df_clean = df_enforced.filter(
    col("purchaseid").isNotNull() &
    col("consumerid").isNotNull()
)


# COMMAND ----------


if not DeltaTable.isDeltaTable(spark, silver_table):
    DeltaTable.createOrReplace(spark).tableName(silver_table).location(silver_table)

delta_table = DeltaTable.forPath(spark, silver_table)
delta_table.alias("t") \
            .merge(
                df_clean.alias("s"),
                "t.consumerid = s.consumerid"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

# COMMAND ----------


