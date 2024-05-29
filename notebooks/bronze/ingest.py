# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC This notebook ingests data from the landing zone in blob storage and creates a Bronze table

# COMMAND ----------

# Widgets
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("bronze_database_name", "")


storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
database_name = dbutils.widgets.get("bronze_database_name")
table_name = "raw_nfl_play_by_play_data"

# COMMAND ----------

# View files
dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/data")

# COMMAND ----------

# Read CSV file
df = spark.read.format("csv").option("header","true").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/data/NFL Play by Play 2009-2018 (v5).csv")
display(df)

# COMMAND ----------

# Save DF as Delta Lake table
df.write.mode("overwrite").format("delta").saveAsTable(f"`{database_name}`.`{table_name}`")
