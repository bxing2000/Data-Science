# Databricks notebook source
print("Hello World!")

# COMMAND ----------

companies_df <- as.data.frame(
  SparkR::selectExpr(
    read.df("/mnt/bronze/cab/billing/companies"), "CompanyId", "AccountId", "CompanyName", "Date(Created) AS DateCreated", "Date(SetupCompletionTime) AS DateSetupCompleted", "TrialExpirationDate AS DateTrialExpired", "TierId")
  )
display(companies_df)