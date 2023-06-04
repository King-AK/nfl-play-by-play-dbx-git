# Databricks notebook source
"""
This notebook ingests data from blob storage
"""

dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("tenant_id", "")


secret_scope = dbutils.widgets.get("secret_scope")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
tenant_id = dbutils.widgets.get("tenant_id")


# COMMAND ----------

# Set up config
app_id = dbutils.secrets.get(scope=secret_scope,key="sp-databricks-poc-app-id")
service_credential = dbutils.secrets.get(scope=secret_scope, key="sp-databricks-poc-app-secret")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", app_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/")
