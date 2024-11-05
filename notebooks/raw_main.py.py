# Databricks notebook source
# MAGIC %md
# MAGIC Feature	trigger(once=True)	trigger(availableNow=True)
# MAGIC Execution Mode	Processes all data once, as a single batch	Processes all available data in multiple micro-batches
# MAGIC Job Behavior	Runs once and stops immediately after processing	Runs until all available data is processed, then stops
# MAGIC Data Processing	Processes all data in a single batch	Processes data in multiple micro-batches
# MAGIC File Discovery	Scans files once at job start	Asynchronously discovers files during job execution
# MAGIC Checkpoints	No checkpoints across runs	Checkpoints between micro-batches
# MAGIC Ideal Use Case	Initial data loads or one-off processing tasks	Catching up on backlog data with rate limiting
# MAGIC Efficiency	May overwhelm resources with large data volumes	Optimizes resource usage by splitting into micro-batches
# MAGIC Rate Limiting Support	No support for rate limiting	Supports rate limiting (e.g., cloudFiles.maxBytesPerTrigger)
# MAGIC Conclusion
# MAGIC trigger(once=True) is best when you want to process all available data at once in a single batch and stop, typically used for one-time loads.
# MAGIC trigger(availableNow=True) is ideal for processing all data available at the start, but in multiple micro-batches, allowing for better resource management and rate limiting.

# COMMAND ----------

dbutils.widgets.text("source_system", "npf")

source_name = dbutils.widgets.get("source_name")

# COMMAND ----------


def copy_metadata_file_to_tmp(filePath, source):
  
  #reading the JSON file that contains the source entity metadata
  #copy the file to local system
  dbutils.fs.cp("dbfs:/mnt/{}".format(filePath),"file:/tmp/{}/{}".format(source,filePath))

# COMMAND ----------

copy_metadata_file_to_tmp("/Volumes/main/test/jsonfiles/bronze_file.json", source_name)


# COMMAND ----------

print(json.dumps("/Volumes/main/test/jsonfiles/landing_to_bronze_file.json"))

# COMMAND ----------


import json
metadata_file_location = "/Volumes/main/test/jsonfiles/landing_to_bronze_file.json"

with open(metadata_file_location, "r") as f_read:
  #file_content = f_read.read()
  data_lake_metadata= json.load(f_read)

source_entities   =[]
source_entities_enabled_flag =[]

for entity in data_lake_metadata["sources"][source_name]["entities"]:
  source_entities.append(entity)
  source_entities_enabled_flag.append(data_lake_metadata["sources"][source_name]["entities"][entity]["enabled"])


# COMMAND ----------

print(source_entities)
print(source_entities_enabled_flag)

# COMMAND ----------

print(source_entities)
