# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML Train
# MAGIC Bootstrap ML Engineering work using AutoML

# COMMAND ----------

# Widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("database", "")
dbutils.widgets.text("table", "")

catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")
table = dbutils.widgets.get("table")

full_table_name = f"{catalog}.{database}.{table}"

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
fe_client = FeatureEngineeringClient()

# Collect team DF
feature_df = fe_client.read_table(name=full_table_name)
display(feature_df)

# COMMAND ----------

train_df, test_df = feature_df.randomSplit([.6,.4], seed = 42)
display(train_df)

# COMMAND ----------

import uuid

exclude_frameworks =  ["sklearn", "xgboost"]
primary_metric = "accuracy"
timeout_minutes = 15
exclude_cols = ["play_id", "game_date", "posteam"]
target_col = "play_type"
experiment_name = f"{full_table_name}_automl_predict{target_col}-{str(uuid.uuid4())}"

# COMMAND ----------

from databricks import automl

summary = automl.classify(train_df, 
                          target_col=target_col, 
                          timeout_minutes=timeout_minutes,
                          exclude_frameworks=exclude_frameworks,
                          experiment_name=experiment_name,
                          primary_metric=primary_metric,
                          exclude_cols=exclude_cols)

# COMMAND ----------

help(summary)

# COMMAND ----------

import mlflow
model_uri = summary.best_trial.model_path
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")
display(test_df.withColumn("play_type_predicted", predict_udf()).select("play_type", "play_type_predicted", "*").orderBy("game_date","play_id"))
