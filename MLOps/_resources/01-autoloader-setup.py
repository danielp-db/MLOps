# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

try:
  dbutils.fs.ls(cloud_storage_path+"/auto_loader/user_json")
  file_exists = True
except:
  file_exists = False
  
if not file_exists or dbutils.widgets.get("reset_all_data") == 'true':
  spark.read.format("json").load("/mnt/field-demos/retail/users_json").repartition(100).write.mode("overwrite").format("json").save(cloud_storage_path+"/auto_loader/user_json")