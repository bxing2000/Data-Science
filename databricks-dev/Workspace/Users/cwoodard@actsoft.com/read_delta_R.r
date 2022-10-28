# Databricks notebook source
# MAGIC %run ./config_sandbox

# COMMAND ----------

dbutils.fs.ls("/mnt/SandboxStorage/SampleDataSets/ByCompany/")

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

#this cell processes timekeeping status submissions
timekeeping_statuses <- SparkR::selectExpr(
  read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/TimekeepingStatus_SampledByCompany_All", source = "delta"),
  "*")
head(timekeeping_statuses)

# COMMAND ----------

users <- SparkR::selectExpr(
  read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/Users_SampledByCompany_All", source = "delta"),
  "CompanyId", "UserId", "Date(CreatedUtc) AS Created", "Date(ModifiedUtc) AS LastModified", "Deleted", "IsActive", "Date(ActivationDate) AS ActivationDate", "Date(DeactivationDate) AS DeactivationDate")
str(users)

# COMMAND ----------

#finally, cell to aggregate all of the usage summaries into a single dataset. this one will be imported into xts for some time series computations in R.  when i have that where i like it i will create a workalike notebook doing the same thing in python