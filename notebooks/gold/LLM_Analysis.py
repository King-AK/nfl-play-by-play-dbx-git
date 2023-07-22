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

# COMMAND ----------

# Give a simple request
nfl_agent.run("how many rows are there?")

# COMMAND ----------

# Let's see how well DaScie does on a simple request.
# dascie.run("Analyze this data, tell me any interesting trends. Make some pretty plots.")

# COMMAND ----------

# Not bad! Now for something even more complex.... can we get out LLM model do some ML!?
# dascie.run(
#     "Train a random forest regressor to predict salary using the most important features. Show me the what variables are most influential to this model"
# )
