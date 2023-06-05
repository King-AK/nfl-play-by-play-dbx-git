# Databricks notebook source
"""
This notebook ingests data from blob storage
"""

dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")


storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

# # Example of how to set up config had cluster not been configured to use secrets
# app_id = dbutils.secrets.get(scope=secret_scope,key="sp-databricks-poc-app-id")
# service_credential = dbutils.secrets.get(scope=secret_scope, key="sp-databricks-poc-app-secret")

# spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", app_id)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", service_credential)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# View files
dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/data")

# COMMAND ----------

# Read CSV file, persist as 
df = spark.read.format("csv").option("header","true").load(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/raw/data/NFL Play by Play 2009-2018 (v5).csv")
display(df)

# COMMAND ----------

# Create external database in storage account
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS ingest_db
             LOCATION 'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/ingest_db' 
             """)

# COMMAND ----------

# Save DF as Delta Lake table
database_name = "ingest_db"
table_name = "raw_nfl_play_by_play_data"
df.write.mode("overwrite").format("delta").saveAsTable(f"`{database_name}`.`{table_name}`")
