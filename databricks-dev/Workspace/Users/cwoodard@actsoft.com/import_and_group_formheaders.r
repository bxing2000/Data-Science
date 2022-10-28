# Databricks notebook source
# MAGIC %run Common/config_All_Zones

# COMMAND ----------

# MAGIC %run ./config_sandbox

# COMMAND ----------

library(base)
library(sparklyr)
library(tidyr)

print("connecting to Databricks")

#make Spark connection
sc <- spark_connect(method = "databricks")

print("Attempting to import Delta Lake file")
#import parquet file
tk_df <- spark_read_delta(sc, "/mnt/SandboxStorage/SampleDataSets/ByCompany/TimekeepingStatus_SampledByCompany_All", name = "tk")

#dump schema
str(companies_df)

#disconnect from Spark
spark_disconnect(sc)