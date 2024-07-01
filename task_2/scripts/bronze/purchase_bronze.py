from task_2.utils.load_utils import load_raw_data


file_path = "s3://tendo-de-test/purchase.csv" # TODO: look for filename patterns for future loads
table_name = "tendo.bronze.purchase"
ddl_path = "ddl/create_bronze_tables.sql"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


load_raw_data(file_path, table_name, ddl_path, checkpoint_path)

