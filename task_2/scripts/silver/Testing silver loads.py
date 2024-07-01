# Databricks notebook source
import os
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import lit, current_timestamp, row_number, col, when
from pyspark.sql.types import StructType, LongType, IntegerType, DateType, TimestampType

def clean_column_names(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        new_col_name = col_name.strip().replace(" ", "_").lower()
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

def add_metadata(df: DataFrame, file_name: str) -> DataFrame:
    return df.withColumn("raw_file_name", lit(file_name)) \
             .withColumn("load_timestamp", current_timestamp())

def log_error(error_message: str, script_name: str, log_dir: str = "/dbfs/logs/"):
    os.makedirs(log_dir, exist_ok=True)
    log_file_name = f"{script_name}_error.log"
    with open(os.path.join(log_dir, log_file_name), "a") as log_file:
        log_file.write(f"{error_message}\n")

def deduplicate_data(df: DataFrame, primary_keys: list) -> DataFrame:
    window_spec = Window.partitionBy(*primary_keys).orderBy(col("load_timestamp").desc())
    return df.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1).drop("row_number")

def validate_and_enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Validate and cast DataFrame columns to the specified schema.
    If casting fails, preserve the original data and flag the row.
    Enforce the schema to ensure the DataFrame adheres to the expected structure.
    """
    for field in schema.fields:
        if isinstance(field.dataType, (LongType, IntegerType, DateType, TimestampType)):
            cast_col = col(field.name).cast(field.dataType)
            df = df.withColumn(f"{field.name}_valid", cast_col.isNotNull())
            df = df.withColumn(field.name, when(cast_col.isNotNull(), cast_col).otherwise(col(field.name)))
        else:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    
    return df


# COMMAND ----------

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType, StringType
from delta.tables import DeltaTable
# from task_2.utils.util import validate_and_enforce_schema, deduplicate_data, log_error

# Define the expected schema
schema = StructType([
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

script_name = "avocado_silver"
spark = SparkSession.builder.appName("AvocadoSilverLayer").getOrCreate()

bronze_table = "tendo.bronze.avocado"
silver_table = "tendo.silver.avocado"




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

# Read data from the Bronze layer
df = spark.read.format("delta").table(bronze_table)

# Add the current timestamp to the updated_at column
df = df.withColumn("updated_at", current_timestamp())

# Validate and enforce schema
df_validated = validate_and_enforce_schema(df, schema)

# Deduplicate data on primary key
df_deduped = deduplicate_data(df_validated, ["purchaseid"])

# Data quality checks
df_clean = df_deduped.filter(
    col("purchaseid").isNotNull() &
    col("consumerid").isNotNull()
)

# Ensure the schema matches before merging
df_clean = df_clean.select([col(field.name) for field in schema.fields])
display(df_clean)

# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
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


# COMMAND ----------


