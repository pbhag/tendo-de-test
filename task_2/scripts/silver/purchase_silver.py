import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.util import deduplicate_data, enforce_schema, log_error

# Define the expected schema
schema = StructType([
    StructField("purchaseid", LongType(), nullable=False),
    StructField("consumerid", LongType(), nullable=False),
    StructField("graphed_date", DateType(), nullable=False),
    StructField("avocado_bunch_id", IntegerType(), nullable=True),
    StructField("reporting_year", IntegerType(), nullable=True),
    StructField("qa_process", StringType(), nullable=True),
    StructField("billing_provider_sku", IntegerType(), nullable=True),
    StructField("grocery_store_id", IntegerType(), nullable=True),
    StructField("price_index", IntegerType(), nullable=True),
    StructField("raw_file_name", StringType(), nullable=True),
    StructField("load_timestamp", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True)  # Add updated_at column
])

def main():
    script_name = "purchase_silver"
    spark = SparkSession.builder.appName("PurchaseSilverLayer").getOrCreate()

    bronze_table = "tendo.bronze.purchase"
    silver_table = "tendo.silver.purchase"

    try:
        # Read data from the Bronze layer
        df = spark.read.format("delta").table(bronze_table)

        # Add the current timestamp to the updated_at column
        df = df.withColumn("updated_at", current_timestamp())

        # Validate and enforce schema
        df_validated = validate_and_enforce_schema(df, schema)

        # Deduplicate data on primary key
        df_deduped = deduplicate_data(df, ["fertilizerid"])

        # Data quality checks
        df_clean = df_enforced.filter(
            col("purchaseid").isNotNull() &
            col("consumerid").isNotNull() 
        )
        # Check if the Silver table exists
        if not DeltaTable.isDeltaTable(spark, silver_table):
            df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        # Merge into Silver table using Delta Lake's merge functionality
        if DeltaTable.isDeltaTable(spark, silver_table):
            delta_table = DeltaTable.forPath(spark, silver_table)
            delta_table.alias("t") \
                       .merge(
                           df_clean.alias("s"),
                           "t.purchaseid = s.purchaseid"
                       ) \
                       .whenMatchedUpdateAll() \
                       .whenNotMatchedInsertAll() \
                       .execute()
        else:
            df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)

        print(f"Data from {bronze_table} upserted to Silver successfully.")
    except Exception as e:
        error_message = f"Error upserting data from {bronze_table} to Silver: {e}"
        print(error_message)
        log_error(error_message, script_name=script_name, log_dir="/dbfs/logs/")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
