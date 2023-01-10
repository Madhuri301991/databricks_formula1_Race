# Databricks notebook source
storage_account_name = "formula1projectdeltalake"
client_id            = dbutils.secrets.get(scope='formula1-scope',key='databricks-app-client-id')
tenant_id            = dbutils.secrets.get(scope='formula1-scope',key='databricks-app-tenant-id')
client_secret        = dbutils.secrets.get(scope='formula1-scope',key='databricks-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#function that mounts storage account in the container 
def mount_adls(container_name):
    dbutils.fs.mount(
       source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
       mount_point = f"/mnt/{storage_account_name}/{container_name}",
       extra_configs = configs)


# COMMAND ----------

mount_adls("raw")
#call function to mount on container named raw 

# COMMAND ----------

mount_adls("processed")
#call function to mount on container named processed

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectdeltalake/raw")
#list to check whether storage account is mounted 

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectdeltalake/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectdeltalake/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectdeltalake/demo")

# COMMAND ----------

#dbutils.fs.unmount("/mnt/formula1projectdeltalake/raw")  
#to unmount 

# COMMAND ----------

#dbutils.fs.unmount("/mnt/formula1projectdeltalake/processed")
#to unmount
