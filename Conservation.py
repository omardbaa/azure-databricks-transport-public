# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Integrate and manage Public Transport data").getOrCreate()

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.omardbstorageaccount.dfs.core.windows.net", 
    "n/M6dGAvjc8505kkOdZWCcfky+UKdckTs1aAhVFqb/J7Ck1yTg+meYzFXNAb/oz1mxXbizdVQdB5+ASt8wSp4w=="
)


# COMMAND ----------


from datetime import datetime

storage_account_name = "omardbstorageaccount"
storage_account_access_key = "n/M6dGAvjc8505kkOdZWCcfky+UKdckTs1aAhVFqb/J7Ck1yTg+meYzFXNAb/oz1mxXbizdVQdB5+ASt8wSp4w=="
container_name = "public-transport-data"

def get_file_path(storage_account_name,storage_account_access_key,container_name):

    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_access_key)

    raw = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public-transport-data/raw/"
    processed = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public-transport-data/processed/"
    archived = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public-transport-data/archive/"
    
    raw_files = dbutils.fs.ls(raw)
    processed_files = dbutils.fs.ls(processed)
    archived_files = dbutils.fs.ls(archived)

    return [raw_files, processed_files, archived_files]

def get_file_duration(path):
    modification_time_ms = path.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to datetime
    duration = (datetime.now() - modification_time).total_seconds() / 60
    print(duration)
    return duration




# COMMAND ----------

def archived_raw_files(raw_paths):
    for path in raw_paths:
        file_duration = get_file_duration(path)
        # check if the duration 
        if file_duration >= 15:
            # get the raw directory
            source_directory = path.path
            # get the archived directory
            destination_directory = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public_transport_data/archive/{path.name}"
            dbutils.fs.mv(source_directory, destination_directory,recurse = True)
# COMMAND ----------

def delete_archived_files(archived_paths):
    for path in archived_paths:
        file_duration = get_file_duration(path)
        # check if the duration 
        if file_duration >= 30:
            # get the raw directory
            source_directory = path.path
            # get the archived directory
            destination_directory = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public_transport_data/archive/{path.name}"
            dbutils.fs.rm(destination_directory,recurse = True)



# get files path
files_paths = get_file_path(storage_account_name,storage_account_access_key,container_name)
print(files_paths)
#archived_raw_files(files_paths[0])
#delete_archived_files(files_paths[2])
