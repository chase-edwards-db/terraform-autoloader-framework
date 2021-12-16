# Databricks notebook source
# MAGIC %md ### Autoloader Stream to Bronze

# COMMAND ----------

# Import necessary libaries and classes

import time

from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, to_date, lit
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

# MAGIC %run "/Users/wayne@fireworkhq.com/DNN/Includes/Utils"

# COMMAND ----------

# sync_id = S3SyncUtil.get_db_sync_id()

# ID to use for testing
static_sync_id = dbutils.widgets.get("static_sync_id")
base_table_name = dbutils.widgets.get("base_table_name")
# static_sync_id = 100000001

# COMMAND ----------

# Configure paths
source_path = f"dbfs:/mnt/cedwards/db_sync/dbs_delta/test_data_source/batch_id={static_sync_id}/*/*.parquet"
sink_path = f"dbfs:/mnt/cedwards/db_sync/dbs_delta/fact_users_bronze_{static_sync_id}/delta/"
checkpoint_path = f"dbfs:/mnt/cedwards/db_sync/dbs_delta/fact_users_bronze_{static_sync_id}/checkpoint/"
table_name = f"{base_table_name}_{static_sync_id}"

# COMMAND ----------

schema = StructType([
#   StructField("batch_id", LongType()),
  StructField("ID", DecimalType(9)),
  StructField("USERNAME", StringType()),
  StructField("NAME", StringType()),
  StructField("AVATAR_KEY", StringType()),
  StructField("SIGNED_UP_AT", TimestampType()),
  StructField("GEO", StringType()),
  StructField("TIME_ZONE", StringType()),
  StructField("LOCALE", StringType()),
  StructField("USER_TYPE", StringType()),
  StructField("LEVEL", DecimalType(38)),
  StructField("FOLLOWERS_COUNT", DecimalType(9)),
  StructField("BUSINESS_ID", DecimalType(4)),
  StructField("DEACTIVATED_AT", TimestampType()),
  StructField("METADATA", StringType()),
  StructField("INSERTED_AT", TimestampType()),
  StructField("UPDATED_AT", TimestampType()),
  StructField("BADGE", StringType()),
  StructField("INSERTED_DATE_PST", DateType()),
  StructField("SIGNED_UP_DATE_PST", DateType()),
  StructField("AGE", DoubleType()),
  StructField("AGE_GROUP", StringType()),
  StructField("GENDER", StringType()),
  StructField("BIRTHDATE", DateType()),
  StructField("REGION", StringType()),
  StructField("LOCALITY", StringType()),
  StructField("POSTAL_CODE", StringType()),
  StructField("COUNTRY", StringType()),
  StructField("BIO", StringType()),
  StructField("LAT", DoubleType()),
  StructField("LONG", DoubleType()),
  StructField("NUMBER", StringType()),
  StructField("INFO", StringType()),
  StructField("RAW_INFO", StringType())
              ])

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.partitionColumns", "") \
  .schema(schema) \
  .load(source_path)

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
  .tableName(table_name) \
  .location(sink_path) \
  .addColumns(df.schema) \
  .property("delta.enableChangeDataFeed", "true") \
  .execute()

# COMMAND ----------


# Define delta table for Merge logic
deltaTable = DeltaTable.forPath(spark, sink_path)

# Define window for deduplication
# This is necessary due to conflicting writes on merge
windowSpec = Window.partitionBy("ID").orderBy(col("UPDATED_AT").desc())

# Define merge logic
def merge_to_delta(microBatchUpdatesDF, batchId):
  deltaTable.alias("fact_users").merge(
    microBatchUpdatesDF
    .dropDuplicates()
    .withColumn("row_number", row_number().over(windowSpec))
    .where(col("row_number") == lit(1))
    .drop("row_number")
    .alias("updates"),
    "fact_users.ID = updates.ID") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


# COMMAND ----------

tic = time.perf_counter()

# Start stream
query = df.writeStream \
  .format("delta") \
  .foreachBatch(merge_to_delta) \
  .option("checkpointLocation", checkpoint_path) \
  .outputMode("append") \
  .trigger(once=True) \
  .start()

query.awaitTermination()

toc = time.perf_counter()
backfill_duration_s = round(toc - tic, 2)
backfill_duration_m = backfill_duration_s / 60

#   .partitionBy("INSERTED_DATE_PST") \
#   .trigger(availableNow=True) \
#   .option("maxFilesPerTrigger", "50") \

# COMMAND ----------

print(f"Backfill job completed in {backfill_duration_s} seconds or {backfill_duration_m} minutes")

# COMMAND ----------


