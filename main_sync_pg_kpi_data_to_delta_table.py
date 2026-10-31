# Databricks notebook source

from databricks.sdk.runtime import spark, dbutils

from src.pg_kpi_data_to_delta_table import config
from src.pg_kpi_data_to_delta_table.pipeline import sync_all

# COMMAND ----------

config.init(spark, dbutils)

# COMMAND ----------

sync_all()
