# Databricks notebook source
# MAGIC %run config_All_Zones

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/cab/billing')

# COMMAND ----------

#load libraries, installing if they're not there
library(dplyr)
library(tidyverse)
library(lubridate)
if(!require("SparkR")) {
  install.packages("SparkR")
}
library(SparkR)

#establish session with Databricks
sc <- sparkR.session(master = "databricks")

# COMMAND ----------

companies_df <- as.data.frame(
  SparkR::selectExpr(
    read.df("/mnt/bronze/cab/billing/companies"), "CompanyId", "AccountId", "CompanyName", "Date(Created) AS DateCreated", "Date(SetupCompletionTime) AS DateSetupCompleted", "TrialExpirationDate AS DateTrialExpired", "TierId")
  )
str(companies_df)

# COMMAND ----------

licenses_df <- as.tibble(
  collect( 
    SparkR::selectExpr(
      read.df("/mnt/bronze/cab/billing/licenses"), "LicenseId", "AccountId", "CompanyId", "InstanceId", "Count", "Date(CreatedOn) AS DateCreated", "Date(ModificationDate) AS DateLstModified", "Date(OriginalActivationDate) AS ActivationDate", "Date(DeactDate) AS DeactivationDate", "OriginalLicenseId")
  )
)
str(licenses_df)

# COMMAND ----------

library(dplyr)
payment_methods_df <- as.tibble(
  collect( SparkR::selectExpr( read.df("/mnt/bronze/cab/billing/payment_methods"), "AccountId", "Billable") )
)
glimpse(payment_methods_df)


# COMMAND ----------

class(payment_methods_df)
billable_company_ids <- payment_methods_df %>% 
  dplyr::filter(Billable == TRUE) %>% 
  dplyr::select(AccountId) %>% unique()
billable_companies_df <- companies_df %>% 
  dplyr::inner_join(billable_company_ids, by = "AccountId")
glimpse(billable_companies_df)

# COMMAND ----------

#collapse license swaps to allow rolling license census as well as churn info
nested_licenses_df <- licenses_df %>%
  dplyr::group_by(AccountId) %>%
  nest() %>% dplyr::rename(Licenses = data)
glimpse(nested_licenses_df)

# COMMAND ----------

processLicenseSwaps <- function(licenses_for_account) {
  #sift into original license rows and swaps
  originalLicenses <- licenses_for_account %>% filter(is.na(OriginalLicenseId))
  swappedLicenses <- licenses_for_account %>% filter(!is.na(OriginalLicenseId))
  if(nrow(swappedLicenses)>0) {
    #process the swapped licenses to get the latest deact date for a license and
    ## of swaps
    summarizedSwaps <- swappedLicenses %>%
      group_by(OriginalLicenseId) %>%
      summarise(DeactivationDate = max(DeactivationDate), SwapCount = n()) %>%
      rename(LicenseId = OriginalLicenseId)
    #left join the summarized licenses onto the original licenses & return
    mergedLicenses <- originalLicenses %>% 
      left_join(summarizedSwaps, by = "LicenseId")
    return(mergedLicenses)
  }
  else {
    return(originalLicenses)
  }
}

processed_licenses_df <- nested_licenses_df %>%
  mutate(
    ProcessedLicenses = map(Licenses, processLicenseSwaps)
  ) %>% select(-Licenses)
