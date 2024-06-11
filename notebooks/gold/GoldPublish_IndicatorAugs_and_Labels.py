# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Publish Indicator Augmentations
# MAGIC
# MAGIC This notebook publishes the gold dataset of NFL data augmented with indicators such as the moving average

# COMMAND ----------

# Widgets
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("silver_database_name", "")
dbutils.widgets.text("gold_database_name", "")

storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
catalog = dbutils.widgets.get("catalog_name")
silver_database_name = dbutils.widgets.get("silver_database_name")
gold_database_name = dbutils.widgets.get("gold_database_name")

gold_table_name = "nfl_play_by_play_data_indicator_label_augmented"
silver_table_name = "nfl_play_by_play_data"

# COMMAND ----------

# Get DF for silver data
silver_full_table_name = f"`{catalog}`.`{silver_database_name}`.`{silver_table_name}`"
silver_df = spark.table(silver_full_table_name)
display(silver_df)

# COMMAND ----------

display(silver_df.where("play_type IN ('pass', 'run', 'punt') AND posteam IN ('STL', 'NE', 'GB')"))

# COMMAND ----------

# Filter down for target columns, play type, and team, add compound_playtype label
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re


def build_compound_playtype(play_type, pass_length, pass_location, run_location, run_gap):
    match play_type:
      case "pass":
        return f"{play_type}|{pass_length}|{pass_location}"
      case "run":
        return f"{play_type}|{run_location}|{run_gap}"
      case _:
        return play_type
build_compound_playtype_udf = F.udf(build_compound_playtype, StringType())

def augment_with_past_ma(df, col, window_size=5):
    # Use of 5 as the MA range is arbitrary here
    lower_bound = (-1) - window_size
    past_ma_window = (Window()
                      .partitionBy(F.col("game_id", "posteam"))
                      .orderBy(F.col("play_id"))
                      .rowsBetween(lower_bound, -1))
    return df.withColumn(f"{col}_{window_size}_play_ma", F.avg(col).over(past_ma_window))

intermediate_df = silver_df
for col in ["yards_gained", "qb_dropback", "qb_scramble", "rush_attempt", "pass_attempt", "sack", "complete_pass"]:
    intermediate_df = augment_with_past_ma(intermediate_df, col, window_size=10)


play_type_filter = ['pass', 'run', 'punt,' 'qb_kneel', 'qb_spike']

gold_df = intermediate_df.filter(intermediate_df.play_type.isin(*play_type_filter))\
                .filter(intermediate_df.sack == 0)\
                .withColumn("compund_playtype", 
                            build_compound_playtype_udf(intermediate_df["play_type"],
                                                        intermediate_df["pass_length"],
                                                        intermediate_df["pass_location"],
                                                        intermediate_df["run_location"],
                                                        intermediate_df["run_gap"]
                                                        )
                            )\
                .withColumn("game_month", F.month(intermediate_df.game_date).cast(StringType()))

display(gold_df)

# COMMAND ----------

# Save DF as Delta Lake table
partition_cols = ["posteam"]
gold_full_table_name = f"`{catalog}`.`{gold_database_name}`.`{gold_table_name}`"
gold_df.write.mode("overwrite").format("delta")\
    .partitionBy(*partition_cols)\
    .option("overwriteSchema", "true").saveAsTable(gold_full_table_name)
