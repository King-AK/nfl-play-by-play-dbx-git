# Databricks notebook source
import os

dbutils.widgets.text("secret_scope", "")
secret_scope = dbutils.widgets.get("secret_scope")

os.environ["OPENAI_API_KEY"] = dbutils.secrets.get(scope=secret_scope, key="openai-api-key")


# COMMAND ----------

# Based on DaScie from Databricks LLM Training
from langchain.llms import OpenAI
from langchain.agents import create_spark_dataframe_agent

df = spark.table("`silver_db`.`silver_play_info_pit`")
nfl_agent = create_spark_dataframe_agent(llm=OpenAI(temperature=0), df=df, verbose=True)
display(df)

# COMMAND ----------

# Give a simple request
nfl_agent.run("how many rows are there?")

# COMMAND ----------

# Get some information regarding play_type
nfl_agent.run("Analyze this data, tell me any interesting trends regarding play_type.")

# COMMAND ----------

# Get some information regarding play_type
nfl_agent.run("""
              Create a new dataframe `df_tmp` which only has the following column: ["play_type"].
              Drop any nulls or NAs from `df_tmp`.
              Convert the dataframe `df_tmp` to a pandas dataframe named `pandas_df`.
              Generate a bar chart using `pandas_df` with matplotlib. This bar chart should be a frequency count of unique play_type values.
              """
              ) 

# COMMAND ----------

# Not bad! Now for something even more complex.... can we get out LLM model do some ML!?
# dascie.run(
#     "Train a random forest regressor to predict salary using the most important features. Show me the what variables are most influential to this model"
# )
