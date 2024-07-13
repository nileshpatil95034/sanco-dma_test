# Databricks notebook source
# MAGIC %md
# MAGIC Mount Nootbook

# COMMAND ----------

# Makr Directory to mount (Optional) 
dbutils.fs.mkdirs('/mnt/adls')

# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

import os

# # Vulnerability Issue Fixed
access_key = dbutils.secrets.get("gen2scope", "access-key-gen2")

# Specify the container and account name
account_name = "insuranceaccount"
container_name = "datacontainer"

# Mount point in DBFS
mount_point = "/mnt/adls"

# Check if the directory is already mounted
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Mount ADLS Gen2 using access key variable
    dbutils.fs.mount(
      source=f"wasbs://{container_name}@{account_name}.blob.core.windows.net",
      mount_point=mount_point,
      extra_configs={f"fs.azure.account.key.{account_name}.blob.core.windows.net": access_key}
    )


# List the mounted directories to verify
display(dbutils.fs.ls(mount_point))


