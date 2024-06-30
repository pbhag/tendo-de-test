import os
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import lit, current_timestamp, row_number, col
from pyspark.sql.types import StructType

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

def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    return df.select([col(field.name).cast(field.dataType) for field in schema.fields])
