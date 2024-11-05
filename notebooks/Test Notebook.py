# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC ***
# MAGIC #Landing Main Notebook
# MAGIC The purpose of this notebook is to take a source and dataset parameter passed from ADF and kick off a child notebook in order to land incremental or snapshot data in /landing
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##### Running helper functions to bring in re-usable functions
# MAGIC ---

# COMMAND ----------

# MAGIC %run /Shared/notebooks/helpers/helper_functions.py

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##### Mounting all containers used in this workflow
# MAGIC ---

# COMMAND ----------

# a. mounting the storage account landing container
mount_container(storage_account_name = "storageaccnt", container_name = "landing", mount_prefix = "data")

# b. mounting the storage account raw container
mount_container(storage_account_name = "storageaccnt", container_name = "raw", mount_prefix = "data")

# c. mounting the storage account pipelines container
mount_container(storage_account_name = "storageaccnt", container_name = "metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##### Getting the source passed from ADF, copying metadata for the source to local storage, and creating a global temp view with unprocessed files
# MAGIC ---

# COMMAND ----------

landing_metadata_file_name = "metadata/landing/source_to_landing_metadata.json"
# getting the source passed from ADF
source_name =  dbutils.widgets.get("adf_source_name")
run_id = dbutils.widgets.get("run_id")

# copy metadata file to local tmp file store
copy_metadata_file_to_tmp(landing_metadata_file_name, source_name)

# COMMAND ----------

import json
with open(f"/tmp/{source_name}/{landing_metadata_file_name}", "r") as f_read:
  source_to_landing_metadata = json.load(f_read)
  
datasets = [entity['sourceLocation'] for entity in source_to_landing_metadata[source_name]['entities']]

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##### Kicking off landing child for any dataset within metadata for a source
# MAGIC   * This will run an incremental or snapshot load into landing
# MAGIC   
# MAGIC ---

# COMMAND ----------

# setting an empty list as notebooks
notebooks = []
 
# creating an input for each dataset in notebooks
for dataset in datasets:
  notebooks.insert(len(notebooks), NotebookData("./landing_child", 9000, {"source": source_name, "dataset": dataset, "run_id": run_id}))

# running the notebooks in parallel
parallelNotebooks(notebooks, 2)

