# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import input_file_name, row_number, col
from pyspark.sql.window import Window

try:
  dbutils.fs.ls(f"{cloud_storage_path}/cdc")
  cdc_files_exist = True
except:
  cdc_files_exist = False
  
if reset_all or not cdc_files_exist:
  #setup data for the full CDC demo
  spark.read.format("csv").schema("name string, address string, email string, id long, operation string, operation_date timestamp").load("/mnt/field-demos/retail/raw_cdc").repartition(50).write.mode("overwrite").format("json").save(f"{cloud_storage_path}/cdc/{dbName}/users")
  spark.read.format("csv").schema("name string, address string, email string, id long, operation string, operation_date timestamp").load("/mnt/field-demos/retail/raw_cdc").repartition(50).write.mode("overwrite").format("json").save(f"{cloud_storage_path}/cdc/{dbName}/transactions")
  print("cdc data updated")