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

# display(silver_df.where("play_type IN ('pass', 'run', 'punt') AND posteam IN ('STL', 'NE', 'GB')"))

# COMMAND ----------

# Helper functions
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

def augment_with_past_ma(df, col, partition_cols=[], window_size=5, col_rename='', order_cols = ["game_date", "play_id"]):
    # Use of 5 as the MA range is arbitrary here
    lower_bound = (-1) - window_size
    past_ma_window = (Window()
                      .partitionBy(*[F.col(c) for c in partition_cols])
                      .orderBy(*[F.col(c) for c in order_cols])
                      .rowsBetween(lower_bound, -1))
    name = col_rename if col_rename != '' else col
    return df.withColumn(f"{name}_{window_size}_play_ma", F.avg(col).over(past_ma_window))
  

def front_fill(df, col, partition_cols=[], order_cols = ["game_date", "play_id"]):
   front_fill_window = (Window()
                        .partitionBy(*[F.col(c) for c in partition_cols])
                        .orderBy(*[F.col(c) for c in order_cols])
                        .rowsBetween(Window.unboundedPreceding, 0))
   return df.withColumn(f"{col}_ffill", F.coalesce(F.col(col), F.last(F.col(col), ignorenulls=True).over(front_fill_window)))
  

# COMMAND ----------

in_game_window_size = 8
defense_window_size = 32

# COMMAND ----------

# Build posteam in-game DF
in_game_intermediate_df = silver_df
for col in ["yards_gained", "qb_dropback", "qb_scramble", "rush_attempt", "pass_attempt", "sack", "complete_pass", "tackled_for_loss"]:
    in_game_intermediate_df = augment_with_past_ma(in_game_intermediate_df, col, partition_cols=["game_id", "posteam"], window_size=in_game_window_size,
                                                   order_cols=["game_date", "play_id"])
    
play_type_filter = ['pass', 'run', 'punt,' 'qb_kneel', 'qb_spike']
gold_in_game_df = in_game_intermediate_df.filter(in_game_intermediate_df.play_type.isin(*play_type_filter))\
                .filter(in_game_intermediate_df.sack == 0)\
                .withColumn("compund_playtype", 
                            build_compound_playtype_udf(in_game_intermediate_df["play_type"],
                                                        in_game_intermediate_df["pass_length"],
                                                        in_game_intermediate_df["pass_location"],
                                                        in_game_intermediate_df["run_location"],
                                                        in_game_intermediate_df["run_gap"]
                                                        )
                            )\
                .withColumn("game_month", F.month(in_game_intermediate_df.game_date).cast(StringType()))

display(gold_in_game_df.filter(gold_in_game_df.posteam=="GB"))

# COMMAND ----------

# Build defensive tracking stats - RUN
run_defense_intermediate_df = silver_df.filter(silver_df.play_type == "run")
for k,v in [("yards_gained", "run_yards_given"), ("tackled_for_loss", "defense_tfl")]:
    run_defense_intermediate_df = augment_with_past_ma(run_defense_intermediate_df, k, partition_cols=["defteam"], window_size=defense_window_size, col_rename=v,
                                                       order_cols=["game_date", "play_id"])
  
run_defense_ma_df = run_defense_intermediate_df.select(
    "play_id",
    "game_date",
    "defteam",
    f"defense_tfl_{defense_window_size}_play_ma",
    f"run_yards_given_{defense_window_size}_play_ma"
)

run_defense_ma_df.cache()

display(run_defense_ma_df.filter(run_defense_ma_df.defteam=="CAR"))

# COMMAND ----------

# Build defensive tracking stats - PASS
pass_defense_intermediate_df = silver_df.filter(silver_df.play_type == "pass")
for k,v in [("yards_gained", "pass_yards_given"), ("sack", "defense_sack"), ("qb_hit", "defense_qb_hit"), ("interception", "defense_interception"), ("complete_pass", "pass_given_up")]:
    pass_defense_intermediate_df = augment_with_past_ma(pass_defense_intermediate_df, k, partition_cols=["defteam"], window_size=defense_window_size, col_rename=v,
                                                        order_cols=["game_date", "play_id"])

pass_defense_ma_df = pass_defense_intermediate_df.select(
    "play_id",
    "game_date",
    "defteam",
    f"pass_yards_given_{defense_window_size}_play_ma",
    f"defense_sack_{defense_window_size}_play_ma",
    f"defense_qb_hit_{defense_window_size}_play_ma",
    f"defense_interception_{defense_window_size}_play_ma",
    f"pass_given_up_{defense_window_size}_play_ma"
)

pass_defense_ma_df.cache()

display(pass_defense_ma_df.filter(pass_defense_ma_df.defteam=="CAR"))

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

# Join run and pass defense to back to the in_game table and front fill defensive stats
gold_df = gold_in_game_df.join(run_defense_ma_df, 
                               on=["play_id", "game_date", "defteam"], 
                               how="left")\
                        .join(pass_defense_ma_df,
                                on=["play_id", "game_date", "defteam"],
                                how="left")

for c in [f"run_yards_given_{defense_window_size}_play_ma",
          f"defense_tfl_{defense_window_size}_play_ma",
          f"pass_yards_given_{defense_window_size}_play_ma",
          f"defense_sack_{defense_window_size}_play_ma",
          f"defense_qb_hit_{defense_window_size}_play_ma",
          f"defense_interception_{defense_window_size}_play_ma",
          f"pass_given_up_{defense_window_size}_play_ma"]:
    gold_df = gold_df.transform(front_fill, col=c, partition_cols=["defteam"], 
                                 order_cols=["game_date", "play_id"])

display(gold_df.filter(gold_df.game_id == "2010100300").orderBy("play_id"))


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

# Define normalization functions


# COMMAND ----------

# Save narrow version of gold table for ml experimentation that omits columns that wont be used in prediction
teams = ["NE", "CHI", "PHI"]
for team in teams:
    narrow_gold_full_table_name = f"`{catalog}`.`{gold_database_name}`.`narrow_ml_{gold_table_name}_{team}`"
    narrow_df.filter(narrow_df.posteam == team)\
        .write.mode("overwrite").format("delta")\
        .option("overwriteSchema", "true").saveAsTable(narrow_gold_full_table_name)
