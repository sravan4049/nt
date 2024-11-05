# Databricks notebook source
config ={
  "zone": "bronze",
  "sources" :{
    "npf":{
        "catalog_name": {
        "dev": "dev-lake-catalog",
        "test": "test-lake-catalog",
        "prod": "prod-lake-catalog"
      },
      "entities":{
        "dtrust_positions":{
            "enabled" :True,
            "file_type": "csv",
            "raw_ingestion_config":{
                "full_lod": True,
                "source_path": "/volume/wdadevcatalog/wdaschema/bronze/landing/npf/",
                "src_checkpoint_path": "/volume/catalog/schema/landing/checpoint",
                "delimiter" : "|",
                "file_does_not_contains_header" : False,
                "ordered_header_list" :["id","name","age","departement"]
            }
        },
        "dtrust_balances":{
            "enabled" :True,
            "file_type": "csv",
            "raw_ingestion_config":{
                "full_lod": True,
                "source_path": "/volume/wdadevcatalog/wdaschema/bronze/landing/npf/",
                "src_checkpoint_path": "/volume/catalog/schema/landing/checpoint",
                "delimiter" : "|",
                "file_does_not_contains_header" : False,
                "ordered_header_list" :["id","name","age","departement"]
            }

        }

      }
    }
}
}
  

# COMMAND ----------


import json
with open("/Volumes/main/test/jsonfiles/landing_to_bronze_file.json", "w") as f:
  json.dump(config, f, indent =4)

# COMMAND ----------

import json
metadata_file_location = "/Volumes/main/test/jsonfiles/landing_to_bronze_file.json"

with open(metadata_file_location, "r") as f_read:
  #file_content = f_read.read()
  data_lake_metadata= json.load(f_read)


# COMMAND ----------

for entity in data_lake_metadata["zone"]["entities"]:
  print(entity)

# COMMAND ----------

config = {
    "zone" : "raw",
"Dataproducts" : {
    "positions":{
        "sources": {
            "npf": {
                "entities": {
                    "dom_trust_positions":{
                        "file_type":"csv",
                        "source_type":"file"
                    },
                    "brokerage_positions":{
                        "file_type":"csv",
                        "source_type":"file"
                    },
                    "loans_psoitions":{
                        "file_type":"csv",
                        "source_type":"file"

                    }
                }
            }
        }
    },
    "loans" :{
        "sources":{
            "npf":{
                "entities":{
                    "home_loans":
                    {
                        "file_type": "csv",
                        "source_type" : "file"
                    },
                    "mortguage_loans":
                    {
                    "file_type":"csv",
                    "source_type":"file"
                    }
                }
            }
        }

    }
    

}

}

# COMMAND ----------

import json
json_string = json.dumps(config)

data = json.loads(json_string)
print(data["Dataproducts"]["positions"])
