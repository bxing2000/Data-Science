# Databricks notebook source
# MAGIC %run /Common/config_All_Zones

# COMMAND ----------

library(purrr)
library(tidyr)
library(ggplot2)
library(readr)
library(lubridate)
library(sparklyr)

sc <- spark_connect(method = "databricks")
s_trips_df <- spark_read_csv(sc, "qa_trips", path = "/FileStore/tables/qacab_trips.csv")
str(s_trips_df)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/cab/")