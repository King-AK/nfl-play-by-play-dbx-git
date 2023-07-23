# Databricks notebook source
"""
This notebook curates the bronze dataset of NFL data into silver datasets
"""

dbutils.widgets.text("target_team", "")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")


storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
target_team = dbutils.widgets.get("target_team")


database_name = "silver_db"
table_name = f"silver_play_info_{target_team}"
bronze_database_name = "bronze_db"
bronze_table_name = "raw_nfl_play_by_play_data"

# COMMAND ----------

# %sql
# --  View Bronze Table
# SELECT * FROM bronze_db.raw_nfl_play_by_play_data WHERE play_type IN ('pass', 'run', 'punt') AND posteam IN ('STL', 'NE', 'GB')

# COMMAND ----------

# Get DF for raw data
raw_df = spark.table("`bronze_db`.`raw_nfl_play_by_play_data`")
# display(raw_df)

# COMMAND ----------

from pyspark.sql.types import *
target_columns = {
    'play_id': IntegerType(),
    'game_id': StringType(),
    'home_team': StringType(),
    'away_team': StringType(),
    'posteam': StringType(),
    'posteam_type': StringType(),
    'defteam': StringType(),
    'side_of_field': StringType(),
    'yardline_100': IntegerType(),
    'game_date': DateType(),
    'quarter_seconds_remaining': IntegerType(),
    'half_seconds_remaining': IntegerType(),
    'game_seconds_remaining': IntegerType(),
    'game_half': StringType(),
    'quarter_end': IntegerType(),
    'drive': IntegerType(),
    'qtr': IntegerType(),
    'down': IntegerType(),
    'goal_to_go': IntegerType(),
    'time': StringType(),
    'yrdln': StringType(),
    'ydstogo': IntegerType(),
    'play_type': StringType(),
    'yards_gained': IntegerType(),
    'shotgun': IntegerType(),
    'no_huddle': IntegerType(),
    'qb_dropback': IntegerType(),
    'qb_scramble': IntegerType(),
    'pass_length': StringType(),
    'pass_location': StringType(),
    'air_yards': IntegerType(),
    'run_location': StringType(),
    'run_gap': StringType(),
    'home_timeouts_remaining': IntegerType(),
    'away_timeouts_remaining': IntegerType(),
    'posteam_timeouts_remaining': IntegerType(),
    'defteam_timeouts_remaining': IntegerType(),
    'total_home_score': IntegerType(),
    'total_away_score': IntegerType(),
    'posteam_score': IntegerType(),
    'defteam_score': IntegerType(),
    'score_differential': IntegerType(),
    # #  'no_score_prob',
    # #  'opp_fg_prob',
    # #  'opp_safety_prob',
    # #  'opp_td_prob',
    # #  'fg_prob',
    # #  'safety_prob',
    # #  'td_prob',
    # #  'extra_point_prob',
    # #  'two_point_conversion_prob',
    'ep': DoubleType(),
    'epa': DoubleType(),
    'total_home_epa': DoubleType(),
    'total_away_epa': DoubleType(),
    'total_home_rush_epa': DoubleType(),
    'total_away_rush_epa': DoubleType(),
    'total_home_pass_epa': DoubleType(),
    'total_away_pass_epa': DoubleType(),
    'air_epa': DoubleType(),
    'yac_epa': DoubleType(),
    'comp_air_epa': DoubleType(),
    'comp_yac_epa': DoubleType(),
    'total_home_comp_air_epa': DoubleType(),
    'total_away_comp_air_epa': DoubleType(),
    'total_home_comp_yac_epa': DoubleType(),
    'total_away_comp_yac_epa': DoubleType(),
    'total_home_raw_air_epa': DoubleType(),
    'total_away_raw_air_epa': DoubleType(),
    'total_home_raw_yac_epa': DoubleType(),
    'total_away_raw_yac_epa': DoubleType(),
    'wp': DoubleType(),
    'def_wp': DoubleType(),
    'home_wp': DoubleType(),
    'away_wp': DoubleType(),
    'wpa': DoubleType(),
    'home_wp_post': DoubleType(),
    'away_wp_post': DoubleType(),
    'total_home_rush_wpa': DoubleType(),
    'total_away_rush_wpa': DoubleType(),
    'total_home_pass_wpa': DoubleType(),
    'total_away_pass_wpa': DoubleType(),
    'air_wpa': DoubleType(),
    'yac_wpa': DoubleType(),
    'comp_air_wpa': DoubleType(),
    'comp_yac_wpa': DoubleType(),
    'total_home_comp_air_wpa': DoubleType(),
    'total_away_comp_air_wpa': DoubleType(),
    'total_home_comp_yac_wpa': DoubleType(),
    'total_away_comp_yac_wpa': DoubleType(),
    'total_home_raw_air_wpa': DoubleType(),
    'total_away_raw_air_wpa': DoubleType(),
    'total_home_raw_yac_wpa': DoubleType(),
    'total_away_raw_yac_wpa': DoubleType(),
    'tackled_for_loss': IntegerType(),
    'interception': IntegerType(),
    'fumble_forced': IntegerType(),
    'qb_hit': IntegerType(),
    'rush_attempt': IntegerType(),
    'pass_attempt': IntegerType(),
    'sack': IntegerType(),
    'complete_pass': IntegerType()
}

# COMMAND ----------

play_type_filter = ['pass', 'run', 'punt,' 'qb_kneel', 'qb_spike']

# COMMAND ----------

# Filter down for target columns, play type, and team
from pyspark.sql import functions as F
select_expr = [F.col(c).cast(t) for c, t in target_columns.items()]
silver_df = raw_df.select(*select_expr)\
                .filter(raw_df.posteam == target_team)\
                .filter(raw_df.play_type.isin(*play_type_filter))

display(silver_df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Augment the dataset with some statistical columns for moving averages

# Consider start of play stats
def augment_with_ma(df, col, window=5):
    # Use of 5 as the MA range is arbitrary here
    lower_bound = 0 - window
    past_ma_window = (Window()
                      .partitionBy(F.col("game_id"))
                      .orderBy(F.col("play_id"))
                      .rowsBetween(lower_bound, 0))
    return df.withColumn(f"{col}_ma", F.avg(col).over(past_ma_window))

for col in ["ydstogo", "no_huddle"]:
    silver_df = augment_with_ma(silver_df, col)

# Consider end of play stats with offset -1 so only past information included
def augment_with_past_ma(df, col, window=5):
    # Use of 5 as the MA range is arbitrary here
    lower_bound = (-1) - window
    past_ma_window = (Window()
                      .partitionBy(F.col("game_id"))
                      .orderBy(F.col("play_id"))
                      .rowsBetween(lower_bound, -1))
    return df.withColumn(f"{col}_ma", F.avg(col).over(past_ma_window))

for col in ["yards_gained", "qb_dropback", "qb_scramble", "rush_attempt", "pass_attempt", "sack", "complete_pass"]:
    silver_df = augment_with_past_ma(silver_df, col)

display(silver_df.orderBy(["game_id", "play_id"]))

# COMMAND ----------

# Create external database in storage account
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {database_name}
             LOCATION 'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{database_name}' 
             """)

# COMMAND ----------

# Save DF as Delta Lake table
silver_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"`{database_name}`.`{table_name}`")
