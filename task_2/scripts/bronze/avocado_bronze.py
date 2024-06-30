from pyspark.sql import SparkSession
from task_2.utils.util import clean_column_names, add_metadata, create_table_if_not_exists, log_error

def main():
    spark = SparkSession.builder.appName("AvocadoBronzeLayer").getOrCreate()

    path = "s3://tendo-de-test/avocado.csv" # TODO: look for filename patterns for future loads
    table_name = "tendo.bronze.avocado"
    ddl_path = "ddl/create_bronze_tables.sql"

    try:
        create_table_if_not_exists(spark, table_name, ddl_path)

        df = spark.read.option("header", "true").csv(path)
        cleaned_df = clean_column_names(df)
        final_df = add_metadata(cleaned_df, path)

        final_df.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"Data from {path} ingested to Bronze successfully.")
    except Exception as e:
        error_message = f"Error ingesting data from {path} to Bronze: {e}"
        print(error_message)
        log_error(error_message)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
