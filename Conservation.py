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
import time

# Montage du Data Lake
storageAccountName = "omardbstorageaccount"
storageAccountAccessKey = "n/M6dGAvjc8505kkOdZWCcfky+UKdckTs1aAhVFqb/J7Ck1yTg+meYzFXNAb/oz1mxXbizdVQdB5+ASt8wSp4w=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-28T15:43:37Z&st=2023-09-27T07:43:37Z&spr=https&sig=13kFGISyoUEheYmR4g9YC3qa4hCoO8YDOEQzIorKfCw%3D"
blobContainerName = "public-transport-data"
mountPoint = "/mnt/public-transport-data/"

# Vérification de l'existence du point de montage
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    try:
        dbutils.fs.mount(
            source="wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
            mount_point=mountPoint,
            extra_configs={'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
        )
        print("Montage réussi !")
    except Exception as e:
        print("Exception lors du montage :", e)
else:
    print("Le point de montage existe déjà.")

def get_file_duration(path):
    modification_time_ms = path.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to datetime
    duration = (datetime.now() - modification_time).total_seconds() / 60
    return duration

def move_processed_files():
    raw_path = mountPoint + "raw/"
    processed_path = mountPoint + "processed/"
    archive_path = mountPoint + "archive/"
    
    processed_files = dbutils.fs.ls(processed_path)
    
    for path in processed_files:
        file_duration = get_file_duration(path)
        # Check if the file is older than 10 minutes
        if file_duration >= 1:
            # Get the source directory
            source_directory = path.path
            # Construct the destination directory in the archive folder
            destination_directory = archive_path + path.name
            
            # Check if the file already exists in the archive directory
            if not dbutils.fs.mv(source_directory, destination_directory):
                # File doesn't exist in the archive, move it
                dbutils.fs.mv(source_directory, destination_directory, recurse=True)

def delete_old_raw_files():
    raw_path = mountPoint + "raw/"
    raw_files = dbutils.fs.ls(raw_path)

    for path in raw_files:
        file_duration = get_file_duration(path)
        # Check if the file is older than 15 minutes
        if file_duration >= 2:
            # Get the source directory
            source_directory = path.path
            # Delete the file
            dbutils.fs.rm(source_directory)

# Initialize a timer
start_time = time.time()

# Call the functions
while True:
    move_processed_files()
    delete_old_raw_files()
    
    # Check if there are no files left and the timer has exceeded 2 minutes
    if len(dbutils.fs.ls(mountPoint + "raw/")) == 0 and len(dbutils.fs.ls(mountPoint + "processed/")) == 0:
        elapsed_time = time.time() - start_time
        if elapsed_time >= 120:
            break
    
    # Sleep for a while before checking again (e.g., every minute)
    time.sleep(60)

