# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Curation
# MAGIC
# MAGIC This notebook curates the bronze dataset of NFL data into the silver dataset

# COMMAND ----------

# Widgets
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("bronze_database_name", "")
dbutils.widgets.text("silver_database_name", "")

storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
target_team = dbutils.widgets.get("target_team")
catalog = dbutils.widgets.get("catalog_name")
silver_database_name = dbutils.widgets.get("silver_database_name")
bronze_database_name = dbutils.widgets.get("bronze_database_name")

silver_table_name = "nfl_play_by_play_data"
bronze_table_name = "nfl_play_by_play_data"

# COMMAND ----------

# Get DF for bronze data
bronze_full_table_name = f"`{catalog}`.`{bronze_database_name}`.`{bronze_table_name}`"
bronze_df = spark.table(bronze_full_table_name)
display(bronze_df)

# COMMAND ----------

# Specify target columns
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
    'no_score_prob': DoubleType(),
    'opp_fg_prob': DoubleType(),
    'opp_safety_prob': DoubleType(),
    'opp_td_prob': DoubleType(),
    'fg_prob': DoubleType(),
    'safety_prob': DoubleType(),
    'td_prob': DoubleType(),
    'extra_point_prob': DoubleType(),
    'two_point_conversion_prob': DoubleType(),
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

# Filter down for target columns, play type, and team
from pyspark.sql import functions as F
select_expr = [F.col(c).cast(t) for c, t in target_columns.items()]
silver_df = bronze_df.select(*select_expr)

display(silver_df)

# COMMAND ----------

# # Save DF as Delta Lake table
partitionCols = ["posteam"]
silver_full_table_name = f"`{catalog}`.`{silver_database_name}`.`{silver_table_name}`"
silver_df.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .partitionBy(*partitionCols)\
    .saveAsTable(silver_full_table_name)
