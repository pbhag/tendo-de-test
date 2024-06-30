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
    StructField("avocado_bunch_id", IntegerType(), nullable=True),
    StructField("plu", LongType(), nullable=True),
    StructField("ripe_index_when_picked", IntegerType(), nullable=True),
    StructField("born_date", DateType(), nullable=True),
    StructField("picked_date", DateType(), nullable=True),
    StructField("sold_date", DateType(), nullable=True),
    StructField("raw_file_name", StringType(), nullable=True),
    StructField("load_timestamp", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True)  # Add updated_at column
])

def main():
    script_name = "avocado_silver"
    spark = SparkSession.builder.appName("AvocadoSilverLayer").getOrCreate()

    bronze_table = "tendo.bronze.avocado"
    silver_table = "tendo.silver.avocado"

    try:
        # Read data from the Bronze layer
        df = spark.read.format("delta").table(bronze_table)

        # Add the current timestamp to the updated_at column
        df = df.withColumn("updated_at", current_timestamp())

        # Deduplicate data on primary key
        df_deduped = deduplicate_data(df, ["consumerid"])

        # Enforce schema
        df_enforced = enforce_schema(df_deduped, schema)

        # Data quality checks
        df_clean = df_enforced.filter(
            col("purchaseid").isNotNull() &
            col("consumerid").isNotNull()
        )

        # Merge into Silver table using Delta Lake's merge functionality
        if DeltaTable.isDeltaTable(spark, silver_table):
            delta_table = DeltaTable.forPath(spark, silver_table)
            delta_table.alias("t") \
                       .merge(
                           df_clean.alias("s"),
                           "t.consumerid = s.consumerid"
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
