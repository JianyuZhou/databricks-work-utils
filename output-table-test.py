# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("input_table_full_name", "", "Input table full name (catalog.schema.table)")
dbutils.widgets.text("output_table_full_name", "", "Output table full name (catalog.schema.table)")
input_table_name = dbutils.widgets.get("input_table_full_name")
output_table_name = dbutils.widgets.get("output_table_full_name")

# COMMAND ----------

print(f"input table name: {input_table_name}")
print(f"output table name: {output_table_name}")

# COMMAND ----------

df = spark.read.table(input_table_name)
df.write.saveAsTable(
  name = output_table_name,
  mode = 'append'
)
