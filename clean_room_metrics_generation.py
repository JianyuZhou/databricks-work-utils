# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Room Metrics Generation
# MAGIC This notebook ingests centralized unity catalog usage logs and updates clean room usage logs every hour.
# MAGIC
# MAGIC Currently we are using staging usage logs as we don't have any production usage of clean room now. Unfortunately, we can't delta streaming `staging.usage_logs` which is a view. In prod, we can instead use
# MAGIC `prod_ds.usage_logs_optimized` which is a delta table. We will migrate this job to using delta streaming when we have clean room usage in production. Know more about usage_logs here: https://databricks.atlassian.net/wiki/spaces/UN/pages/38084230/Usage+Logs

# COMMAND ----------

env = "staging"
clean_room_table_base_dir = '/clean_room_kpi/tables'
clean_room_checkpoint_base_dir = '/clean_room_kpi/checkpoints'
table_name = 'clean_room_usage_logs'
table_location = f'{clean_room_table_base_dir}/{table_name}'

# COMMAND ----------

# # NOTE: This cell is executed only for backfill purpose!
# spark.sql("CREATE SCHEMA IF NOT EXISTS clean_room_kpi")
# spark.sql(f"""CREATE TABLE IF NOT EXISTS clean_room_kpi.{table_name} USING DELTA LOCATION '{table_location}'""")
# from pyspark.sql import functions as F

# env = "staging"
# # Get the current timestamp, truncated to the hour
# end_time = F.unix_timestamp(F.date_trunc('hour', F.current_timestamp())) * 1000

# # Calculate the start time (one hour before the end time)
# hour = 60 * 60 * 1000
# day = hour * 24
# week = day * 7
# start_time = end_time - 4 * week

# from pyspark.sql.functions import col
# uc_usage_logs_df = spark.read.format("delta").table(f"{env}.usage_logs_stream") \
#                         .filter(col("date") >= F.date_sub(F.current_date(),28)) \
#                         .filter((col("timestamp") >= start_time) & (col("timestamp") <= end_time)) \
#                         .filter(col("metric") == "unityCatalogEvent")

# clean_room_usage_logs_df = uc_usage_logs_df.filter((col("tags").getItem("opType").like("%CleanRoom%")))
# clean_room_usage_logs_df.write \
#                           .format("delta") \
#                           .partitionBy("date") \
#                           .mode("overwrite") \
#                           .option("txnVersion", 1) \
#                           .option("overwriteSchema", "true") \
#                           .option("txnAppId", f"{start_time}-{end_time}") \
#                           .option("mergeSchema", "true") \
#                           .save(table_location)

# COMMAND ----------

# We can't use delta streaming here due to usage_logs_stream/usage_logs is a view which is not support by stream reader yet.
# To provide streaming-like functionality, we manually create a checkpoint using log's timestamp in batch processing
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime

checkpoint_location = f'{clean_room_checkpoint_base_dir}/{table_name}'
dbutils.fs.mkdirs(clean_room_checkpoint_base_dir) # mkdirs is idempotent

try:
  # Try to read the timestamp from the checkpoint file
  checkpoint_timestamp = dbutils.fs.head(checkpoint_location)
  checkpoint_timestamp = int(checkpoint_timestamp.strip())
except Exception as e:
  # If the checkpoint file does not exist or is empty, default to the timestamp of 2023-07-17 20:59:48 UTC. This is the latest timestamp right after backfill completed, we will start from there
  # If checkpoint is somehow deleted by someone, this can be manually set to some other appropriate values to resume batch processing. Duplicates/missing data may happen
  # when resetting hte checkpoint_timestamp, just be careful to set a reasonable value to avoid missing too much data or too much duplicates.
  checkpoint_timestamp = 1689627588300

# Filter UC usage logs
uc_usage_logs_df = spark.read.format("delta").table(f"{env}.usage_logs_stream") \ # use usage_logs_stream instead of usage_logs for lower latency
                        .filter(col("metric") == "unityCatalogEvent") \
                        .filter(col("timestamp") > checkpoint_timestamp) # we may miss a few data here but should be fine as it won't be too much
# Filter clean room usage logs
clean_room_usage_logs_df = uc_usage_logs_df.filter((col("tags").getItem("opType").like("%CleanRoom%")))

# COMMAND ----------

table_location = f'{clean_room_table_base_dir}/{table_name}'
clean_room_usage_logs_df.write \
                          .format("delta") \
                          .partitionBy("date") \
                          .mode("append") \
                          .option("mergeSchema", "true") \
                          .save(table_location)
# Persist checkpoint to dbfs
max_timestamp = uc_usage_logs_df.agg({"timestamp": "max"}).collect()[0][0]
# Convert the timestamp to a string
checkpoint_timestamp_str = str(max_timestamp)
# Write the checkpoint timestamp to the file
dbutils.fs.put(checkpoint_location, checkpoint_timestamp_str, overwrite=True)
print("Completed!")
