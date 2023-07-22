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

target_columns = ['play_id',
 'game_id',
 'home_team',
 'away_team',
 'posteam',
 'posteam_type',
 'defteam',
 'side_of_field',
 'yardline_100',
 'game_date',
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
 'yrdln',
 'ydstogo',
 'play_type',
 'shotgun',
 'no_huddle',
 'pass_length',
 'pass_location',
 'air_yards',
 'run_location',
 'run_gap',
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
 'ep',
 'epa',
 'total_home_epa',
 'total_away_epa',
 'total_home_rush_epa',
 'total_away_rush_epa',
 'total_home_pass_epa',
 'total_away_pass_epa',
 'air_epa',
 'yac_epa',
 'comp_air_epa',
 'comp_yac_epa',
 'total_home_comp_air_epa',
 'total_away_comp_air_epa',
 'total_home_comp_yac_epa',
 'total_away_comp_yac_epa',
 'total_home_raw_air_epa',
 'total_away_raw_air_epa',
 'total_home_raw_yac_epa',
 'total_away_raw_yac_epa',
 'wp',
 'def_wp',
 'home_wp',
 'away_wp',
 'wpa',
 'home_wp_post',
 'away_wp_post',
 'total_home_rush_wpa',
 'total_away_rush_wpa',
 'total_home_pass_wpa',
 'total_away_pass_wpa',
 'air_wpa',
 'yac_wpa',
 'comp_air_wpa',
 'comp_yac_wpa',
 'total_home_comp_air_wpa',
 'total_away_comp_air_wpa',
 'total_home_comp_yac_wpa',
 'total_away_comp_yac_wpa',
 'total_home_raw_air_wpa',
 'total_away_raw_air_wpa',
 'total_home_raw_yac_wpa',
 'total_away_raw_yac_wpa'
]

# COMMAND ----------

play_type_filter = ['pass', 'run', 'punt,' 'qb_kneel', 'qb_spike']

# COMMAND ----------

silver_df = raw_df.select(*target_columns)\
                .filter(raw_df.posteam == target_team)\
                .filter(raw_df.play_type.isin(*play_type_filter))

display(silver_df)

# COMMAND ----------

# Create external database in storage account
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {database_name}
             LOCATION 'abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{database_name}' 
             """)

# COMMAND ----------

# Save DF as Delta Lake table
silver_df.write.mode("overwrite").format("delta").saveAsTable(f"`{database_name}`.`{table_name}`")
