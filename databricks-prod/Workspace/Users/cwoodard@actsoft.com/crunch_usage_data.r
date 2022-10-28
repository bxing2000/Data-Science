# Databricks notebook source
# MAGIC %run ./config_All_Zones_A

# COMMAND ----------

library(tidyverse)
library(lubridate)
if(!require("SparkR")) {
  install.packages("SparkR")
}
library(SparkR)
library(purrr)
library(data.table)
sc <- sparkR.session(master = "databricks")

# COMMAND ----------

#########################################################################################################################
# aggregate account and company level data for churn determination
#########################################################################################################################

non_test_account_ids_df <- as.data.frame(SparkR::selectExpr(read.df("/mnt/silver/cab/accounts", source = "delta"), "AccountId AS DynamicsAccountId", "BillingAccountId AS AccountId", "IsTest", "Status")) %>%
    dplyr::filter(FALSE == IsTest) %>% dplyr::select(AccountId) %>% unique()
billable_account_ids_df <- as.data.frame(SparkR::selectExpr( read.df("/mnt/bronze/cab/billing/payment_methods"), "AccountId", "Billable")) %>%
    dplyr::filter(TRUE == Billable) %>% dplyr::select(AccountId) %>% unique()
billable_nontest_account_ids_df <- non_test_account_ids_df %>% inner_join(billable_account_ids_df) %>% unique()

licenses_df <- as.data.frame(SparkR::selectExpr(read.df("/mnt/silver/cab/licenses", source = "delta"), "LicenseId", "AccountId", "CompanyId", "InstanceId", "TierId", "CreatedOn", "Date(DeactDate) AS DeactivationDate", "OriginalLicenseId", "Date(OriginalActivationDate) AS ActivationDate", "ProductId")) %>% inner_join(billable_nontest_account_ids_df, by = "AccountId")

churn_df <- licenses_df %>% 
  dplyr::group_by(AccountId, CompanyId) %>% 
  summarise(Activated = min(ActivationDate), Deactivated = max(DeactivationDate)) %>% 
  dplyr::mutate(
    YearActivated = lubridate::year(Activated), MonthActivated = lubridate::month(Activated),
    YearDeactivated = lubridate::year(Deactivated), MonthDeactivated = lubridate::month(Deactivated),
    Churned = !is.na(Deactivated)
  )

companies_df <- as.data.frame(SparkR::selectExpr(
    read.df("/mnt/silver/cab/companies", source = "delta"), "AccountId", "InstanceId", "InstanceName", "CompanyId", "CompanyName", "Date(BillingCreatedUtc) AS DateCreated", "IsActive", "IsPending", "TierId", "TierName", "VerticalId", "VerticalName")) %>% 
  inner_join(billable_nontest_account_ids_df, by = "AccountId") %>% 
  left_join(churn_df, by = c("AccountId", "CompanyId")) %>% mutate(Gap = ActivationDate - DateCreated)

# COMMAND ----------

form_headers_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/form_headers", source = "delta"), "CompanyId", "StartedByUserId AS UserId", "Year(CreatedUtc) AS Year", "Month(CreatedUtc) As Month", "1 AS Hashmark" )
form_headers_r_df <- as.data.frame(form_headers_df)
str(form_headers_r_df)

# COMMAND ----------

#########################################################################################################################
# read and aggregate usage data
#########################################################################################################################

all_trips_spark_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/trips", source = "delta"), "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "DistanceMiles", "1 AS Hashmark")

order_headers_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/order_headers/", source = "delta"), "CompanyId", "Date(CreatedUtcTime) AS UsageDate", "CreatedByUserId AS UserId", "1 AS Hashmark")

att_timekeeping_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/timekeeping_statuses/", source = "delta"), "CompanyId", "UserId", "Date(CreatedUtc) AS UsageDate", "1 AS Hashmark")

att_messages_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/sent_messages/", source = "delta"), "CompanyId", "FromUserId AS UserId", "Date(SentTimeUtc) AS UsageDate", "1 AS Hashmark")

# COMMAND ----------

#########################################################################################################################
# aggregate account and company level data for churn determination
#########################################################################################################################

