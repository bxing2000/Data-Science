# Databricks notebook source
library(dplyr)
library(sparklyr)

# COMMAND ----------

df <- data.frame( categ = c("A", "B", "C"), contin = c(1.0, 2.2, 1.5))
str(df)