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
