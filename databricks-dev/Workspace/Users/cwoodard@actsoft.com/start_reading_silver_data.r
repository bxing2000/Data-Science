# Databricks notebook source
# MAGIC %run /Common/config_All_Zones

# COMMAND ----------

dbutils.fs.ls("/mnt/sandboxes/SampleDataSets/ByCompany")

# COMMAND ----------

#move the R environment prep here
if(!require("SparkR")) {
  install.packages("SparkR")
  library(SparkR)
}
else {
  library(SparkR)
}

library(dplyr)
library(tidyr)
library(lubridate)

#establish session with Databricks
sc <- sparkR.session(master = "databricks")

# COMMAND ----------

df <- df.read
sparkR.session.stop()
