import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable
from task_2.utils.util import validate_and_enforce_schema, deduplicate_data, log_error

# Define the expected schema
avocado_schema = StructType([
    StructField("purchaseid", LongType(), False),
    StructField("consumerid", LongType(), False),
    StructField("avocado_bunch_id", IntegerType(), True),
    StructField("plu", IntegerType(), True),
    StructField("ripe_index_when_picked", IntegerType(), True),
    StructField("born_date", DateType(), True),
    StructField("picked_date", DateType(), True),
    StructField("sold_date", DateType(), True),
    StructField("raw_file_name", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)  # Add updated_at column
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

        # Validate and enforce schema
        df_validated = validate_and_enforce_schema(df, avocado_schema)

        # Deduplicate data on primary key
        df_deduped = deduplicate_data(df_validated, ["purchaseid"])

        # Data quality checks
        df_clean = df_deduped.filter(
            col("purchaseid").isNotNull() &
            col("consumerid").isNotNull()
        )

        # Check if the Silver table exists
        if not DeltaTable.isDeltaTable(spark, silver_table):
            df_clean.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        else:
            # Merge into Silver table using Delta Lake's merge functionality
            delta_table = DeltaTable.forName(spark, silver_table)
            (delta_table.alias("t")
                .merge(
                    df_clean.alias("s"),
                    "t.consumerid = s.consumerid AND t.purchaseid = s.purchaseid"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        print(f"Data from {bronze_table} upserted to Silver table: {silver_table}")

    except Exception as e:
        error_message = f"Error upserting data from {bronze_table} to Silver: {e}"
        print(error_message)
        log_error(error_message, script_name=script_name, log_dir="/dbfs/logs/")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
