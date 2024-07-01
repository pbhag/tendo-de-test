from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


spark = SparkSession.builder.appName("RawIngest").getOrCreate()


def create_table_if_not_exists(spark: SparkSession, table_name: str, ddl_path: str):
    if not spark.catalog.tableExists(table_name):
        with open(ddl_path, 'r') as ddl_file:
            ddl_query = ddl_file.read()
        spark.sql(ddl_query)
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists.")


def load_raw_data(s3_directory, filename, table_name, ddl_path, checkpoint_path):
    # Create table if it doesnt exist already, with DDL
    create_table_if_not_exists(spark, table_name, ddl_path) 

    # Configure Auto Loader to ingest CSV data to a Delta table
    (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.includePatterns", f"{filename}")
    .option("header", "true")
    .load(s3_directory)
    .select("*", 
            col("_metadata.file_path").alias("source_file"), 
            current_timestamp().alias("processing_time"))
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name))
