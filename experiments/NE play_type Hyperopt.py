# Databricks notebook source
from databricks.feature_engineering import FeatureEngineeringClient
fe_client = FeatureEngineeringClient()

# Collect NE DF 
drop_cols = ["play_id", "game_date", "pos_team"]
full_table_name = "nfl-play-by-play-project.gold.narrow_ml_nfl_play_by_play_data_indicator_label_augmented_ne"
feature_df = fe_client.read_table(name=full_table_name)\
    .drop(*drop_cols)
display(feature_df)

# COMMAND ----------

# TODO: more intermediate transformations for data numerical and non-numerical ahead of hyperopt training

# COMMAND ----------

import mlflow 
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd
from sklearn.model_selection import train_test_split
from hyperopt import fmin, tpe, hp, SparkTrials

mlflow.autolog()

# COMMAND ----------

# Split data for pandas
label_col = "play_type"
random_state = 42
df = feature_df.toPandas()
X_train, X_test, y_train, y_test = train_test_split(df.drop([label_col], axis=1), df[label_col], random_state=random_state)

# COMMAND ----------

# Define objective function
def RFObjective(params):
    model = RandomForestClassifier(n_estimators=int(params["n_estimators"]), 
                                  max_depth=int(params["max_depth"]), 
                                  min_samples_leaf=int(params["min_samples_leaf"]),
                                  min_samples_split=int(params["min_samples_split"]))
    X_train = params["X_train"]
    y_train = params["y_train"]
    score_function = params["score_function"]
    model.fit(X_train, y_train)
    pred = model.predict(X_train)
    score = score_function(y_train, pred)

    # Hyperopt minimizes score, here we minimize mse. 
    return score

# COMMAND ----------

# Define search space
search_space = {"X_train": X_train,
                "y_train": y_train,
                "score_function": accuracy_score,
                "n_estimators": hp.quniform("n_estimators", 100, 500, 5),
                "max_depth": hp.quniform("max_depth", 5, 20, 1),
                "min_samples_leaf": hp.quniform("min_samples_leaf", 1, 5, 1),
                "min_samples_split": hp.quniform("min_samples_split", 2, 6, 1)}

# Set parallelism (should be order of magnitude smaller than max_evals)
spark_trials = SparkTrials(parallelism=2)

with mlflow.start_run(run_name="Hyperopt"):
    argmin = fmin(fn=RFObjective,
                  space=search_space,
                  algo=tpe.suggest,
                  max_evals=16,
                  trials=spark_trials)
