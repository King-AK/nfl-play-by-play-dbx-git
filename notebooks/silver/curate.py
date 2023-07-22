# Databricks notebook source
"""
This notebook curates the bronze dataset of NFL data into silver datasets
"""

# dbutils.widgets.text("storage_account_name", "")
# dbutils.widgets.text("container_name", "")


# storage_account_name = dbutils.widgets.get("storage_account_name")
# container_name = dbutils.widgets.get("container_name")

database_name = "bronze_db"
table_name = "raw_nfl_play_by_play_data"

# COMMAND ----------

# MAGIC %sql
# MAGIC --  View Bronze Table
# MAGIC SELECT * FROM bronze_db.raw_nfl_play_by_play_data
