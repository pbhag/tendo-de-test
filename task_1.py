# Databricks notebook source
from pyspark.sql import SparkSession
import re

# Load raw data from S3
avocado_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/avocado.csv")
consumer_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/consumer.csv")
fertilizer_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/fertilizer.csv")
purchase_df = spark.read.format("csv").option("header", "true").load("s3://tendo-de-test/purchase.csv")


# COMMAND ----------

print("avocado")
avocado_df.printSchema()
print("consumer")
consumer_df.printSchema()
print("fertilizer")
fertilizer_df.printSchema()
print("purchase")
purchase_df.printSchema()

# COMMAND ----------

# Count rows
print("Avocado rows:", avocado_df.count())
print("Consumer rows:", consumer_df.count())
print("Fertilizer rows:", fertilizer_df.count())
print("Purchase rows:", purchase_df.count())

# COMMAND ----------

# Check for null values
from pyspark.sql.functions import col, isnan, when, count

print("Avocado")
avocado_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in avocado_df.columns]).show()
print("Consumer")
consumer_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in consumer_df.columns]).show()
print("Fertilizer")
fertilizer_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in fertilizer_df.columns]).show()
print("Purchase")
purchase_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in purchase_df.columns]).show()


# COMMAND ----------

# Summary statistics
print("avocado")
avocado_df.describe().show()
print("consumer")
consumer_df.describe().show()
print("fertilizer")
fertilizer_df.describe().show()
print("purchase")
purchase_df.describe().show()

# COMMAND ----------

# Check for duplicates with respect to the primary key of each file
print("Avocado duplicates:", avocado_df.count() - avocado_df.dropDuplicates().count())
print("Consumer duplicates:", consumer_df.count() - consumer_df.dropDuplicates().count())
print("Fertilizer duplicates:", fertilizer_df.count() - fertilizer_df.dropDuplicates().count())
print("Purchase duplicates:", purchase_df.count() - purchase_df.dropDuplicates().count())

# COMMAND ----------

# Check for duplicates in Avocado table
duplicate_avocado = avocado_df.groupBy("purchaseid").count().filter(col("count") > 1)
if duplicate_avocado.count() > 0:
    print("Duplicate entries found in Avocado table based on purchaseid:")
    duplicate_avocado.show()
else:
    print("No duplicates found in Avocado table based on purchaseid.")

# Check for duplicates in Consumer table
duplicate_consumer = consumer_df.groupBy("consumerid").count().filter(col("count") > 1)
if duplicate_consumer.count() > 0:
    print("Duplicate entries found in Consumer table based on consumerid:")
    duplicate_consumer.show()
else:
    print("No duplicates found in Consumer table based on consumerid.")
    
# Check for duplicates in Fertilizer table
duplicate_fertilizer = fertilizer_df.groupBy("fertilizerid").count().filter(col("count") > 1)
if duplicate_fertilizer.count() > 0:
    print("Duplicate entries found in Fertilizer table based on fertilizerid:")
    duplicate_fertilizer.show()
else:
    print("No duplicates found in Fertilizer table based on fertilizerid.")

# Check for duplicates in Purchase table
duplicate_purchase = purchase_df.groupBy("purchaseid").count().filter(col("count") > 1)
if duplicate_purchase.count() > 0:
    print("Duplicate entries found in Purchase table based on purchaseid:")
    duplicate_purchase.show()
else:
    print("No duplicates found in Purchase table based on purchaseid.")



# COMMAND ----------

# MAGIC %md
# MAGIC Initial observations:
# MAGIC
# MAGIC - Some column names have spaces (needs to be standarized to snake case to load nicely into data warehouse):
# MAGIC   - `avocado.ripe index when picked` needs to be snake case
# MAGIC   - `purchase.QA process` needs to be snake case
# MAGIC
# MAGIC - All columns are string type (will need to investigate what types to convert to)
# MAGIC - Avocado 
# MAGIC   - has no null values
# MAGIC   - issues in data - min `picked_date` is before min `born_date` which doesn't make sense, data issue. Also data types dont match (date vs timestamp)
# MAGIC   - will need to implement logic to check that `picked_date` is after `born_date`, and that `sold_date` is also after `picked_date` to get valid data
# MAGIC - Consumer has a null for `age`, but that might not be an issue
# MAGIC   - `age` min is 190, max is 36 - data type issue
# MAGIC - Fertilizer has a null value for `purchaseid` and `consumerid`, both of which are foriegn keys that are to be NOT NULL. No nulls for primary key `fertilizerid`
# MAGIC   - `mg` is mostly NULL and has tons of variance 
# MAGIC   - `frequency` is not standardized at all, need to understand how this is measured/recorded
# MAGIC   - several duplicates with respect to primary key, half the data is duplicated 
# MAGIC - Purchase has a null value for primary key `purchaseid` and foreign key `consumerid` (which is also to be NOT NULL). Many other nulls in other columns

# COMMAND ----------

# MAGIC %md
# MAGIC Creating cleaned up versions of the data

# COMMAND ----------

from pyspark.sql.functions import col

# Standardize column names
def standardize_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.strip().lower().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

avocado_df = standardize_column_names(avocado_df)
consumer_df = standardize_column_names(consumer_df)
fertilizer_df = standardize_column_names(fertilizer_df)
purchase_df = standardize_column_names(purchase_df)

# COMMAND ----------

from pyspark.sql.functions import trim, col, when
# Show the original DataFrame
print("Original DataFrame:")
consumer_df.show()

# Clean and convert the column to integer, falling back to original if cast results in NULL
consumer_df_cleaned = (consumer_df
                       .withColumn("consumerid_trimmed", trim(col("consumerid")))
                       .withColumn("consumerid_int",
                                   when(col("consumerid_trimmed").cast("long").isNotNull(),
                                        col("consumerid_trimmed").cast("long"))
                                   .otherwise(col("consumerid_trimmed")))
                       .drop("consumerid_trimmed"))

# Show the cleaned DataFrame
print("Cleaned DataFrame:")
consumer_df_cleaned.show()
consumer_df_cleaned.printSchema()

# Conclusion: raw data consumerid cannot be converted to int, or long. Will stay string

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, regexp_replace
from pyspark.sql.types import LongType


# Typing columns 
avocado_df_clean = avocado_df.select(
    col("consumerid").cast("string"),
    col("purchaseid").cast("integer"),
    col("avocado_bunch_id").cast("integer"),
    col("plu").cast("integer"),
    col("ripe_index_when_picked").cast("integer"),
    col("born_date").cast("date"),
    col("picked_date").cast("date"),
    col("sold_date").cast("date")
)

consumer_df_clean = consumer_df.select(
    col("consumerid").cast("string"),
    col("sex").cast("string"),
    col("ethnicity").cast("string"),
    col("race").cast("string"),
    col("age").cast("integer")
)

fertilizer_df_clean = fertilizer_df.select(
    col("fertilizerid").cast("integer"),
    col("purchaseid").cast("integer"),
    col("consumerid").cast("string"),
    col("type").cast("string"),
    col("mg").cast("integer"),

)

purchase_df_clean = purchase_df.select(
    col("consumerid").cast("string"),
    col("purchaseid").cast("integer"),
    col("graphed_date").cast("date"),
    col("reporting_year").cast("integer"),
    col("qa_process").cast("string"),
    col("billing_provider_sku").cast("integer"),
    col("grocery_store_id").cast("integer"),
    col("price_index").cast("integer")
)

avocado_df_clean.show()
consumer_df_clean.show()
fertilizer_df_clean.show()
purchase_df_clean.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the output file
# MAGIC
# MAGIC | Field Name | Comments |
# MAGIC | -------- | ------- |
# MAGIC | consumer_id | Consumer id |
# MAGIC | Sex | Consumer sex |
# MAGIC | age | Consumer age |
# MAGIC | avocado_days_sold | Avocado age in days | 
# MAGIC | avocado_ripe_index | Values: 0-10 |
# MAGIC | avodcado_days_picked | |
# MAGIC | fertilizer_type | |
# MAGIC
# MAGIC Questions: 
# MAGIC - How is avocado_days_sold defined? From born_date to sold_date? or from born_date to pick_date?
# MAGIC - How is avocado_days_picked defined? From pick_date to sold_date?

# COMMAND ----------

from pyspark.sql.functions import col, datediff

avocado_df_clean = avocado_df_clean.filter((col("picked_date") > col("born_date")) & 
                                           (col("sold_date") > col("picked_date")))
avocado_df_clean = avocado_df_clean.withColumn("avocado_days_sold", datediff(col("sold_date"), col("born_date")))
avocado_df_clean = avocado_df_clean.withColumnRenamed("ripe_index_when_picked", "avocado_ripe_index")
avocado_df_clean = avocado_df_clean.withColumn("avocado_days_picked", datediff(col("sold_date"), col("picked_date")))

fertilizer_df_clean = fertilizer_df.drop_duplicates(subset=['fertilizerid'])
fertilizer_df_clean = fertilizer_df_clean.filter((col("consumerid").isNotNull()) & (col("purchaseid").isNotNull()))
fertilizer_df_clean = fertilizer_df_clean.withColumnRenamed("type", "fertilizer_type")
# Purchase has a null value for primary key purchaseid and foreign key consumerid 
purchase_df_clean = purchase_df_clean.filter((col("purchaseid").isNotNull()) & (col("consumerid").isNotNull()))

avocado_df_clean.show()
consumer_df_clean.show()
fertilizer_df_clean.show()
purchase_df_clean.show()


# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

# Generate the final DataFrame
# Join the tables
# Join consumer and purchase tables on consumerid
consumer_purchase_df = consumer_df.join(purchase_df, on="consumerid", how="outer")

# Join the result with avocado table on purchaseid
consumer_purchase_avocado_df = consumer_purchase_df.join(avocado_df, on=["purchaseid"], how="outer")

# Join the result with fertilizer table on purchaseid
final_df = consumer_purchase_avocado_df.join(fertilizer_df, on=["purchaseid"], how="outer")

# Show the joined DataFrame
final_df.show()

output_df = (purchase_df_clean.alias("p")
    .join(consumer_df_clean, on=["consumerid"], how="left")
    .join(fertilizer_df_clean, on=["purchaseid", "consumerid"], how="left")
    .join(avocado_df_clean, on=["purchaseid", "consumerid"], how="left")
    .select(
        col("p.consumerid"),
        col("sex"),
        col("age"),
        col("avocado_days_sold"),
        col("avocado_ripe_index"), 
        col("avocado_days_picked"),
        col("fertilizer_type")
    ))

display(output_df)




# COMMAND ----------

from datetime import datetime

# Get the current date
current_date = datetime.now().date()

# Write the output to a CSV file
iteration = 1
output_df.write.format("csv") \
    .option("delimiter", "|") \
    .option("quote", "\"") \
    .option("header", "true") \
    .option("encoding", "ASCII") \
    .option("lineSep", "\n") \
    .mode("overwrite") \
    .save(f"s3://tendo-de-test/target_{iteration}_{current_date}.csv")




# COMMAND ----------


