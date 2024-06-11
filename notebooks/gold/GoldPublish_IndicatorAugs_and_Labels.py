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
                      .partitionBy(F.col("game_id"), F.col("posteam"))
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

partition_cols = ["posteam"]
narrow_table_columns = [
 'home_team',
 'away_team',
 'posteam',
 'posteam_type',
 'defteam',
 'side_of_field',
 'yardline_100',
 'quarter_seconds_remaining',
 'half_seconds_remaining',
 'game_seconds_remaining',
 'game_half',
 'quarter_end',
 'drive',
 'qtr',
 'down',
 'goal_to_go',
 'time',
 'ydstogo',
 'play_type',
 'yards_gained',
 'shotgun',
 'no_huddle',
 'home_timeouts_remaining',
 'away_timeouts_remaining',
 'posteam_timeouts_remaining',
 'defteam_timeouts_remaining',
 'total_home_score',
 'total_away_score',
 'posteam_score',
 'defteam_score',
 'score_differential',
#  'no_score_prob',
#  'opp_fg_prob',
#  'opp_safety_prob',
#  'opp_td_prob',
#  'fg_prob',
#  'safety_prob',
#  'td_prob',
#  'extra_point_prob',
#  'two_point_conversion_prob',
#  'ep',
#  'epa',
#  'total_home_epa',
#  'total_away_epa',
#  'total_home_rush_epa',
#  'total_away_rush_epa',
#  'total_home_pass_epa',
#  'total_away_pass_epa',
#  'air_epa',
#  'yac_epa',
#  'comp_air_epa',
#  'comp_yac_epa',
#  'total_home_comp_air_epa',
#  'total_away_comp_air_epa',
#  'total_home_comp_yac_epa',
#  'total_away_comp_yac_epa',
#  'total_home_raw_air_epa',
#  'total_away_raw_air_epa',
#  'total_home_raw_yac_epa',
#  'total_away_raw_yac_epa',
#  'wp',
#  'def_wp',
#  'home_wp',
#  'away_wp',
#  'wpa',
#  'home_wp_post',
#  'away_wp_post',
#  'total_home_rush_wpa',
#  'total_away_rush_wpa',
#  'total_home_pass_wpa',
#  'total_away_pass_wpa',
#  'air_wpa',
#  'yac_wpa',
#  'comp_air_wpa',
#  'comp_yac_wpa',
#  'total_home_comp_air_wpa',
#  'total_away_comp_air_wpa',
#  'total_home_comp_yac_wpa',
#  'total_away_comp_yac_wpa',
#  'total_home_raw_air_wpa',
#  'total_away_raw_air_wpa',
#  'total_home_raw_yac_wpa',
#  'total_away_raw_yac_wpa',
 'yards_gained_10_play_ma',
 'qb_dropback_10_play_ma',
 'qb_scramble_10_play_ma',
 'rush_attempt_10_play_ma',
 'pass_attempt_10_play_ma',
 'sack_10_play_ma',
 'complete_pass_10_play_ma',
#  'compund_playtype', # TODO reintroduce with additional window tracking compound play types as flag vals since theres no hash marker col
 'game_month']

# COMMAND ----------

# Save gold table as Delta Lake table
partition_cols = ["posteam"]
gold_full_table_name = f"`{catalog}`.`{gold_database_name}`.`{gold_table_name}`"
gold_df.write.mode("overwrite").format("delta")\
    .partitionBy(*partition_cols)\
    .option("overwriteSchema", "true").saveAsTable(gold_full_table_name)

# COMMAND ----------

narrow_df = gold_df.select(*narrow_table_columns)
display(narrow_df)

# COMMAND ----------

# Save narrow version of gold table for ml experimentation that omits columns that wont be used in prediction
narrow_gold_full_table_name = f"`{catalog}`.`{gold_database_name}`.`narrow_{gold_table_name}`"
narrow_df.write.mode("overwrite").format("delta")\
    .partitionBy(*partition_cols)\
    .option("overwriteSchema", "true").saveAsTable(narrow_gold_full_table_name)
