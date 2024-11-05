# Databricks notebook source
import json
json_data = '{"person": {"name": "John", "age": 30}}'
data = json.loads(json_data)

# COMMAND ----------

import json
df =spark.read.json("/Volumes/main/test/jsonfiles/")
#df.printSchema()

display(df)

# COMMAND ----------

import json


# COMMAND ----------

multiline_df = spark.read.option("multiline","true") \
      .json("/Volumes/main/test/jsonfiles/multiline-zipcode.json")
display(multiline_df)

# COMMAND ----------

import json
food_ratings = {"organic dog food": 2, "human food": 10}
json.dumps(food_ratings)


# COMMAND ----------

config = {
  "zone": "bronze",
  "source_system": "npf",
  "source_path": "/volume/wdadevcatalog/wdaschema/bronze/landing/npf/",
  "source_type":"file",


  "source_tables": [
    {
    "name" :"dtrust_postions",
    "enabled": "true", 
    "file_type":"csv",
    "delimiter":"|",
    "header":"False",
    "src_checkpoint_path" :"/volume/catalog/schema/landing/checpoint",
    "ordered_header_list":["id","name","age","departement"]
      },

      {
        "name" :"dtrust_balances",
        "enabled": "true", 
        "file_type":"csv",
        "delimiter":"|",
        "header":"False",
        "ordered_header_list":["id","name","location","state"]
          }
        ],
    "target_schema" :"bronze_account"
  
  
}               




# COMMAND ----------


with open("/Volumes/main/test/jsonfiles/bronze_file.json", "w") as f:
  json.dump(config, f, indent =4)

# COMMAND ----------

import json
import pandas as pd
file_path="/Volumes/main/test/jsonfiles/bronze_file.json"

df = pd.read_json(file_path)
display(df.head())

# COMMAND ----------

import json
import pandas as pd
file_path="/Volumes/main/test/jsonfiles/bronze_file.json"

df = pd.read_json(file_path)
#display(df.head())
# multiline_df = spark.read.option("multiline","true") \
#       .json("/Volumes/main/test/jsonfiles/bronze_file.json")
# display(multiline_df)

from pyspark.sql.functions import explode, col

# Explode the source_tables array into separate rows
flattened_df = multiline_df.withColumn("source_tables", explode("source_tables"))

# Select the nested fields from the exploded source_tables column
flattened_df = flattened_df.select(
    col("source_path"),
    col("source_system"),
    col("zone"),
    col("source_tables.delimiter").alias("delimiter"),
    col("source_tables.file_type").alias("file_type"),
    col("source_tables.header").alias("header"),
    col("source_tables.name").alias("source_table"),
    col("source_tables.ordered_header_list").alias("ordered_header_list")
)
display(flattened_df)

# COMMAND ----------

import json
notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

print(notebook_info)

# COMMAND ----------

import json
data = {
  "use":{
    "name": "sravan",
    "age" :20
  }
}


with open("/Volumes/main/test/jsonfiles/testfile.json", "w") as f:
  json.dump(data, f, indent =4)



# COMMAND ----------

json_str = json.dumps(data, indent =4)
print(json_str  )

# COMMAND ----------

import json
import requests

response = requests.get("https://jsonplaceholder.typicode.com/todos")
todos =json.loads(response.text)

todos_by_user = {}
for todo in todos:
  if todo["completed"]:
    try:
      todos_by_user[todo["userId"]] += 1
    ex ept KeyError:
      todos_by_user[todo["userId"]] = 1






# COMMAND ----------

json_data = 
{
  "zone": "bronze",
  "source_system": "npf",
  "source_path": "/volume/wdadevcatalog/wdaschema/bronze/landing/npf/",
  "source_tables": [
    {
      "name": "dtrust_positions",
      "file_type": "csv",
      "delimiter": "|",
      "header": "False",
      "ordered_header_list": ["id", "name", "age", "department"]
    },
    {
      "name": "dtrust_balances",
      "file_type": "csv",
      "delimiter": "|",
      "header": "False",
      "ordered_header_list": ["id", "name", "location", "state"]
    }
  ]
}

