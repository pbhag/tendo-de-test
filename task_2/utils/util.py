from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
import os

def clean_column_names(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        new_col_name = col_name.strip().replace(" ", "_").lower()
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

def add_metadata(df: DataFrame, file_name: str) -> DataFrame:
    return df.withColumn("raw_file_name", lit(file_name)) \
             .withColumn("load_timestamp", current_timestamp())

def create_table_if_not_exists(spark: SparkSession, table_name: str, ddl_path: str):
    if not spark.catalog.tableExists(table_name):
        with open(ddl_path, 'r') as ddl_file:
            ddl_query = ddl_file.read()
        spark.sql(ddl_query)
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists.")

def log_error(error_message: str, log_dir: str = "logs/"):
    os.makedirs(log_dir, exist_ok=True)
    with open(os.path.join(log_dir, "error.log"), "a") as log_file:
        log_file.write(f"{error_message}\n")
