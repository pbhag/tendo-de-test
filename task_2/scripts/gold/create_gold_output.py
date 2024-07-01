import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_timestamp
from delta.tables import DeltaTable
from task_2.utils.util import log_error

def main():
    script_name = "generate_gold"
    spark = SparkSession.builder.appName("GenerateGoldLayer").getOrCreate()

    silver_consumer_table = "tendo.silver.consumer"
    silver_purchase_table = "tendo.silver.purchase"
    silver_avocado_table = "tendo.silver.avocado"
    silver_fertilizer_table = "tendo.silver.fertilizer"
    gold_table = "tendo.gold.final_output"

    try:
        # Read data from the Silver tables
        consumer_df = spark.read.format("delta").table(silver_consumer_table)
        purchase_df = spark.read.format("delta").table(silver_purchase_table)
        avocado_df = spark.read.format("delta").table(silver_avocado_table)
        fertilizer_df = spark.read.format("delta").table(silver_fertilizer_table)

        # Perform necessary transformations
        avocado_df = avocado_df.withColumn("avocado_days_sold", datediff(col("sold_date"), col("born_date")))
        avocado_df = avocado_df.withColumnRenamed("ripe_index_when_picked", "avocado_ripe_index")
        avocado_df = avocado_df.withColumn("avocado_days_picked", datediff(col("sold_date"), col("picked_date")))

        fertilizer_df = fertilizer_df.withColumnRenamed("type", "fertilizer_type")

        # Join the tables
        consumer_purchase_df = consumer_df.join(purchase_df, on="consumerid", how="outer")
        consumer_purchase_avocado_df = consumer_purchase_df.join(avocado_df, on=["purchaseid"], how="outer")
        final_df = consumer_purchase_avocado_df.join(fertilizer_df, on=["purchaseid"], how="outer")

        # Select the required columns and add updated_at column
        output_df = final_df.select(
            col("consumerid"),
            col("sex"),
            col("age"),
            col("avocado_days_sold"),
            col("avocado_ripe_index"),
            col("avocado_days_picked"),
            col("fertilizer_type")
        ).withColumn("updated_at", current_timestamp())

        # Ensure the schema matches before merging
        output_df = output_df.select(
            col("consumerid").cast("long"),
            col("sex").cast("string"),
            col("age").cast("integer"),
            col("avocado_days_sold").cast("integer"),
            col("avocado_ripe_index").cast("integer"),
            col("avocado_days_picked").cast("integer"),
            col("fertilizer_type").cast("string"),
            col("updated_at").cast("timestamp")
        )

        output_df = 

        # Check if the Gold table exists
        if not spark.catalog.tableExists(gold_table):
            output_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
        else:
            # Merge into Gold table using Delta Lake's merge functionality
            delta_table = DeltaTable.forName(spark, gold_table)
            delta_table.alias("t") \
                .merge(
                    output_df.alias("s"),
                    "t.consumerid = s.consumerid"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

        print(f"Data upserted to Gold table: {gold_table}")

    except Exception as e:
        error_message = f"Error generating gold data: {e}"
        print(error_message)
        log_error(error_message, script_name=script_name, log_dir="/dbfs/logs/")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
