# Databricks notebook source
# MAGIC %pip install git+https://github.com/databricks/clean-room-orchestration

# COMMAND ----------

from databricks_clean_room_orchestrator.client import CleanRoomRestClient
from databricks_clean_room_orchestrator.client import Resource
client = CleanRoomRestClient()

# COMMAND ----------

station_name = "test-station-3"

# COMMAND ----------

# step 1: create clean room station
client.createStation("jianyu-test", station_name, "aws:us-east-1:3de21b35-76a1-4197-98e5-b58f76d8fc5f", "output-table-test", {"input_table_full_name":"la.lb.lc", "output_table_full_name":"aa.b.c"}, {"aa.b.c":"main.default.students"})

# COMMAND ----------

client.setupStationResource("jianyu-test", station_name, Resource.METASTORE)

# COMMAND ----------

client.getStationWorkspaceStatus("jianyu-test", station_name)

# COMMAND ----------

client.setupStationResource("jianyu-test", station_name, Resource.COLLABORATOR_SHARES)

# COMMAND ----------

client.setupStationResource("jianyu-test", "test-station", Resource.NOTEBOOK_SERVICE_PRINCIPAL)

# COMMAND ----------


