# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=features