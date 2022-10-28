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

nta <- filter(selectExpr(read.df("/mnt/silver/cab/accounts", source = "delta"), "AccountId AS DynamicsAccountId", "BillingAccountId AS AccountId", "IsTest", "Status AS AccountStatus"), "IsTest == FALSE")

bl <- filter(selectExpr(read.df("/mnt/silver/cab/licenses", source = "delta"), "LicenseId", "AccountId", "CompanyId", "InstanceId", "TierId", "CreatedOn", "Date(DeactDate) AS DeactivationDate", "OriginalLicenseId", "Date(OriginalActivationDate) AS ActivationDate", "ProductId", "Billable", "Count"), "Billable == TRUE")

#summarize to get activation/deactivation dates for company
sbl <- agg(
  group_by(bl, "AccountId", "InstanceId", "CompanyId"), 
  Activated = min(bl$ActivationDate),
  Deactivated = max(bl$DeactivationDate),
  Count = sum(bl$Count)
)
sbl$Churned = isNotNull(sbl$Deactivated)
sbl_df <- as.data.frame(sbl)

companies <- as.data.frame(SparkR::selectExpr(
    read.df("/mnt/silver/cab/companies", source = "delta"), "AccountId", "InstanceId", "CompanyId", "CompanyName", "Date(BillingCreatedUtc) AS DateCreated", "IsActive", "IsPending", "TierId", "VerticalId", "Date(SetupCompletionTime) AS SetupCompletionDate"))

# COMMAND ----------

companies_df <- companies %>% 
  inner_join(as.data.frame(nta), by = "AccountId") %>%
  inner_join(sbl_df, by = c("AccountId", "InstanceId", "CompanyId"))
companies_date_range <- companies_df %>% dplyr::select(CompanyId, Activated, Deactivated)
str(companies_date_range)

# COMMAND ----------

fh <- SparkR::selectExpr(read.df("/mnt/silver/cab/form_headers", source = "delta"), 
    "CompanyId", "StartedByUserId AS UserId", "Date(CreatedUtc) AS UsageDate", "1 AS Hashmark" )
form_headers_df <- agg(
  group_by(fh, "CompanyId", "UsageDate", "UserId"), 
  FormCount = sum(fh$Hashmark))
str(form_headers_df)

# COMMAND ----------

tp <- SparkR::selectExpr(SparkR::read.df("/mnt/silver/cab/trips", source = "delta"), "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "1 AS Hashmark")
trips_df <- agg(group_by(tp, "CompanyId", "UsageDate", "UserId"), TripCount = sum(tp$Hashmark))
str(trips_df)

# COMMAND ----------

#messages
ms <- selectExpr(read.df("/mnt/silver/cab/sent_messages", source = "delta"), "CompanyId", "InstanceId", "FromUserId AS UserId", "Date(SentTimeUtc) AS UsageDate", "1 AS Hashmark")
messages_df <- agg(group_by(ms, "CompanyId", "UserId", "UsageDate"), MessageCount =sum(ms$Hashmark) )
str(messages_df)

# COMMAND ----------

#timekeeping/
tk <- selectExpr(read.df("/mnt/silver/cab/timekeeping_statuses", source = "delta"), "CompanyId", "InstanceId", "UserId", "Date(CreatedUtc) AS UsageDate", "1 AS Hashmark")
timekeeping_df <- agg(group_by(tk, "CompanyId", "UserId", "UsageDate"), StatusCount = sum(tk$Hashmark))
str(timekeeping_df)

# COMMAND ----------

#orders order_headers
oh <- selectExpr(read.df("/mnt/silver/cab/order_headers", source = "delta"), "CompanyId", "InstanceId", "CurrentUserId AS UserId", "Date(CurrentStatusStartUtcTime) AS UsageDate", "1 AS Hashmark")
order_headers_df <- agg(group_by(oh, "CompanyId", "UserId", "UsageDate"), StatusCount = sum(oh$Hashmark))
rm(oh)
str(order_headers_df)

# COMMAND ----------

#check#1 - which billable ecompanies have no usage records at all?  pull CompanyIds from usage counters and full join them together and unique them,
#then anti join them with the company ids.
#1 - extract CompanyId usage dataframes and unique them, then rbind() them and unique() the whole thing.
usage_co_ids <- as.data.frame(unique(
  rbind(
    selectExpr(form_headers_df, "CompanyId"),
    selectExpr(trips_df, "CompanyId"),
    selectExpr(messages_df, "CompanyId"),
    selectExpr(timekeeping_df, "CompanyId"),
    selectExpr(order_headers_df, "CompanyId")
  )
))

#2 - get company ids from companies table
co_co_ids <- companies[,c("CompanyId")]

#3 - 

# COMMAND ----------

form_headers_date_range <- agg( group_by(form_headers_df, "CompanyId"), FirstFormSubmission = min(form_headers_df$UsageDate), LastFormSubmission = max(form_headers_df$UsageDate))
str(form_headers_date_range)
#inner join with companies_df to see which ones have submission dates outside of their active range