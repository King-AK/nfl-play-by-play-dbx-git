# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC This notebook ingests data from the landing zone in blob storage and creates a Bronze table

# COMMAND ----------

# Widgets
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("bronze_database_name", "")
dbutils.widgets.text("raw_data_relative_path", "")


storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
catalog_name = dbutils.widgets.get("catalog_name")
database_name = dbutils.widgets.get("bronze_database_name")
raw_data_relative_path = dbutils.widgets.get("raw_data_relative_path")

table_name = "nfl_play_by_play_data"

raw_data_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{raw_data_relative_path}"

# COMMAND ----------

# View files
dbutils.fs.ls(raw_data_path)

# COMMAND ----------

# Read CSV file
df = spark.read.format("csv").option("header","true").load(raw_data_path)
display(df)

# COMMAND ----------

# Save DF as Delta Lake table
full_table_name = f"`{catalog_name}`.`{database_name}`.`{table_name}`"
# print(full_table_name)
df.write.mode("overwrite").format("delta").saveAsTable(full_table_name)
