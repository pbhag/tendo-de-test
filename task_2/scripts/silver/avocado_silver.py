import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, TimestampType
from util.utils import deduplicate_data, enforce_schema

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
    StructField("load_timestamp", TimestampType(), True)
])

@dlt.table(
    name="avocado_silver",
    comment="Silver layer table for Avocado data with schema enforcement, deduplication, and data quality checks"
)
def avocado_silver():
    # Read data from the Bronze layer
    df = spark.read.format("delta").table("tendo_de_bronze.avocado")

    # Deduplicate data
    df_deduped = deduplicate_data(df, ["purchaseid", "consumerid"])

    # Enforce schema
    df_enforced = enforce_schema(df_deduped, avocado_schema)

    # Data quality checks
    df_clean = df_enforced.filter(
        F.col("purchaseid").isNotNull() &
        F.col("consumerid").isNotNull()
    )

    return df_clean

@dlt.table(
    name="avocado_silver_upsert",
    comment="Upserted Silver layer table for Avocado data"
)
def avocado_silver_upsert():
    from delta.tables import DeltaTable
    
    df_clean = dlt.read("avocado_silver")

    # Merge into Silver table using Delta Lake's merge functionality
    delta_table = DeltaTable.forName(spark, "tendo_de_silver.avocado")

    delta_table.alias("t") \
               .merge(
                   df_clean.alias("s"),
                   "t.purchaseid = s.purchaseid AND t.consumerid = s.consumerid"
               ) \
               .whenMatchedUpdateAll() \
               .whenNotMatchedInsertAll() \
               .execute()

    return df_clean
