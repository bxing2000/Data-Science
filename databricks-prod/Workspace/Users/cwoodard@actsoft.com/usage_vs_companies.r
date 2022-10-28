# Databricks notebook source
# MAGIC %run ./config_All_Zones_A

# COMMAND ----------

dbutils.fs.ls('/mnt/silver/cab')

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
  dplyr::mutate(Churned = !is.na(Deactivated))

companies_df <- as.data.frame(SparkR::selectExpr(
    read.df("/mnt/silver/cab/companies", source = "delta"), "AccountId", "InstanceId", "InstanceName", "CompanyId", "CompanyName", "Date(BillingCreatedUtc) AS DateCreated", "IsActive", "IsPending", "TierId", "TierName", "VerticalId", "VerticalName")) %>% 
  inner_join(billable_nontest_account_ids_df, by = "AccountId") %>% 
  left_join(churn_df, by = c("AccountId", "CompanyId"))

# COMMAND ----------

#########################################################################################################################
# read and aggregate usage data
#########################################################################################################################
form_headers_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/form_headers", source = "delta"), "CompanyId", "StartedByUserId AS UserId", "Year(CreatedUtc) AS Year", "Month(CreatedUtc) As Month", "1 AS Hashmark" )

all_trips_spark_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/trips", source = "delta"), "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "DistanceMiles", "1 AS Hashmark")

order_headers_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/order_headers/", source = "delta"), "CompanyId", "Date(CreatedUtcTime) AS UsageDate", "CreatedByUserId AS UserId", "1 AS Hashmark")

att_timekeeping_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/timekeeping_statuses/", source = "delta"), "CompanyId", "UserId", "Date(CreatedUtc) AS UsageDate", "1 AS Hashmark")

att_messages_df <- SparkR::selectExpr(read.df("/mnt/silver/cab/sent_messages/", source = "delta"), "CompanyId", "FromUserId AS UserId", "Date(SentTimeUtc) AS UsageDate", "1 AS Hashmark")

# COMMAND ----------

##########################################################################################
#get # of companyIds in each usage counter and compare them to total number of companies
fh_company_ids <- unique(SparkR::select(form_headers_df, "CompanyId"))
tr_company_ids <- unique(SparkR::select(all_trips_spark_df, "CompanyId"))
ms_company_ids <- unique(SparkR::select(att_messages_df, "CompanyId"))
tk_company_ids <- unique(SparkR::select(att_timekeeping_df, "CompanyId"))
or_company_ids <- unique(SparkR::select(order_headers_df, "CompanyId"))

# COMMAND ----------

print(
  paste(
    'FH company ids', nrow(fh_company_ids), '\n',
    'TR company ids', nrow(tr_company_ids), '\n',
    'MS company ids', nrow(ms_company_ids), '\n',
    'TK company ids', nrow(tk_company_ids), '\n',
    'OR company ids', nrow(or_company_ids), '\n',
    'Total companies', nrow(companies_df)
  )
)