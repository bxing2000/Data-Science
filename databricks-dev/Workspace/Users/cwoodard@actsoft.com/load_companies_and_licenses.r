# Databricks notebook source
# MAGIC %md
# MAGIC Finding Which Companies Have Churned
# MAGIC ------------------------------------
# MAGIC 
# MAGIC Author:  Christopher Woodard
# MAGIC 
# MAGIC Date:    Feb 10, 2020
# MAGIC 
# MAGIC Purpose: Demonstrate reading Delta Lake files in R using SparkR

# COMMAND ----------

# MAGIC %run ./config_sandbox

# COMMAND ----------

baseExtractPath <- "/dbfs/FileStore/Rextracts"


# COMMAND ----------

# MAGIC %fs ls "/mnt/SandboxStorage/SampleDataSets/"

# COMMAND ----------

#pull in the /mnt/SandboxStorage/SampleDataSets/ByCompany
#   CompanyModules_SampledByCompany_All
#   FormHeaders_SampledByCompany_All
#   OrderHeaders_SampledByCompany_All
#   OrderStatus_SampledByCompany_All
#   TimekeepingStatus_SampledByCompany_All
#   Trips_SampledByCompany_All/
#   Users_SampledByCompany_All/

# COMMAND ----------

#we need the SparkR package to let R communicate with Databricks' underlying Apache Spark framework.  If it isn't installed in the cluster, install and then load.
#otherwise, just laod it.
if(!require("SparkR")) {
  install.packages("SparkR")
  library(SparkR)
}
else {
  library(SparkR)
}

#establish session with Databricks
sc <- sparkR.session(master = "databricks")

# COMMAND ----------

#read from the Companies_All delta lake dataset and just bring in CompanyId, CompanyName, CreatedDate, City, RegionName and CountryCode.  They aren't all necessary for analysis yet, but they
#will be useful for annotating future output.  plus, it makes a nice demo.  read.df() brings the data back as a SparkDataFrame object, which needs to be converted to an R data frame so 
#it can be processed futher.  

str(as.data.frame(read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Companies_All", source = "delta")))

#all_companies_df <- as.data.frame(
#  SparkR::selectExpr(
#    read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Companies_All", source = "delta"),
#    "CompanyId", "concat('Company ', CompanyId) AS CompanyName", "Date(CreatedUtc) AS CreatedDate", "City", "RegionName", #"CountryCode", "VerticalId"
#  )
#)
#companiesExtractPath <- paste(baseExtractPath, "All_Companies.rds", sep = "/")
#saveRDS(all_companies_df, file = companiesExtractPath)

# COMMAND ----------

#read from the Licenses_All dataset and just bring in the company id, original activation date and deactivation date.  we're not playing with the Tier or LicenseId just yet; we really
#just need to know which companies are active and which ones are not (have churned)
adf <- read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Licenses_All/", source = "delta")
str(adf)
all_licenses_df <- as.data.frame(
  SparkR::selectExpr(read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Licenses_All/", source = "delta"),"CompanyId", "LicenseId", "AccountId", "Status", "TierId", "Count", "Date(OriginalActivationDate) AS ActivationDate", "Date(DeactDate) AS DeactivationDate"))
licensesExtractPath <- paste(baseExtractPath, "All_Licenses.rds", sep = "/")
saveRDS(all_licenses_df, file = licensesExtractPath)

# COMMAND ----------

library(dplyr)
library(lubridate)

#now to figure out active and inactive companies for churn.  per JamesA, a company is inactive if it has no active licenses. to find this out, we take the all_licenses_df and pipe it
#through dplyr's mutate function to set "license_active" equal to TRUE if the license's deactivation date is NA (missing) and FALSE otherwise.  Then we pipe the output from that operation
#through group_by(CompanyId) and then through the summarise() function (one of R's main aggregation functions).  summarise() will aggregate values within groups defined by the group_by()
#variable (or variables, since you can use more than one grouping variable if need be).  To count active licenses, we sum up the values of license_active (TRUE maps to a numeric 1) and then
#count up the total number of license table rows per company.  After that, we can compute the number of inactive licenses as the difference between total licenses and active licenses.
#Since we're interested in churn, we also create an indicator "churned" which is true if there are no active licenses; since we also want to know how long a churned company was with us,
#we (in the summarise() function) get the earliest activation date and latest deactivation date.  The difference is the company's tenure.

#(%>% operator pipes one operation into another if it's been written to work with %>%)

company_licenses_df <- all_licenses_df %>% mutate(license_active = is.na(DeactivationDate)) %>%
  group_by(CompanyId) %>%
  summarise( 
    active_licenses = sum(license_active),
    total_licenses = n(),
    earliest_activation = min(ActivationDate), 
    latest_deactivation = max(DeactivationDate)
  ) %>%
  mutate(
    churned = active_licenses == 0, 
    inactive_licenses = total_licenses - active_licenses, 
    tenure_in_days = as.numeric(latest_deactivation - earliest_activation)
  ) %>%
  as_tibble()
aggregated_licenses_path <- paste(baseExtractPath, "Aggregated_Licenses.rds", sep = "/")
saveRDS(company_licenses_df, file = aggregated_licenses_path)

# COMMAND ----------

#load verticals
all_verticals_df <- as.data.frame(
  SparkR::selectExpr(
    read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Verticals_full_2019/", source = "delta"),
    "VerticalId", "ParentVerticalId", "Name AS VerticalName"
  )
)
all_verticals_path <- paste(baseExtractPath, "AllVerticals.rds", sep = "/")
saveRDS(all_verticals_df, file = all_verticals_path)

# COMMAND ----------

#glue verticals onto companies df
companies_with_verticals_df <- all_companies_df %>% left_join(all_verticals_df, by = "VerticalId")
str(companies_with_verticals_df)
companies_with_verticals_path <- paste(baseExtractPath, "CompaniesWithVerticals.rds", sep = "/")
saveRDS(companies_with_verticals_df, file = companies_with_verticals_path)

# COMMAND ----------

#now glue the companies with verticals onto the licenses, so we can get some stats on the verticals most likely to churn.
#one more thing we might try is active_license / total_license as well.
fully_merged_df <- company_licenses_df %>%
  left_join(companies_with_verticals_df, by = "CompanyId") %>%
  filter(!is.na(CompanyName))
str(fully_merged_df)
fully_merged_path <- paste(baseExtractPath, "CompaniesLicensesAndVerticals.rds", sep = "/")
saveRDS(fully_merged_df, file = fully_merged_path)

# COMMAND ----------

#further thoughts - after we get the usage dataset done, we can build a nested tibble with a row for each company and columns
#containing the company info, churn info (flag, date brackets), form submission counts, order status counts, timekeeping
#status counts and trip counts.  we can do one of these for all of the sampled companies to get an idea how much space that
#will take, and then we can expand on that format to make extracts that can be downloaded to BI machines.
#
#will need to do some space optimization - vertical names, city names, state names, etc will need to be as.factor()'d to 
#save storage.
#
#

# COMMAND ----------

formHeaders_all_df <- as.data.frame(SparkR::selectExpr(
    read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/FormHeaders_SampledByCompany_All", source = "delta"),
    "CompanyId", "StartedByUserId AS UserId", "Date(StartUtcTimeTag) AS UsageDate", "FormHeaderId"))
formHeadersExtractPath <- paste(baseExtractPath, "formHeaders.rds", sep = "/")
saveRDS(formHeaders_all_df, file = formHeadersExtractPath)

# COMMAND ----------

trips_df <- as.data.frame(SparkR::selectExpr(
  read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/Trips_SampledByCompany_All", source = "delta"),
  "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "StartLat"))
tripsExtractPath <- paste(baseExtractPath, "trips.rds", sep = "/")
saveRDS(trips_df, file = tripsExtractPath)

# COMMAND ----------

order_statuses_df <- as.data.frame(SparkR::selectExpr(
  read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/OrderStatus_SampledByCompany_All", source = "delta"),
  "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "OrderStatusId"))
orderStatusesExtractPath <- paste(baseExtractPath, "orderStatuses.rds", sep = "/")
saveRDS(order_statuses_df, file = orderStatusesExtractPath)

# COMMAND ----------

timekeeping_statuses_df <- as.data.frame(SparkR::selectExpr(
  read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/TimekeepingStatus_SampledByCompany_All", source = "delta"),
  "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "TimekeepingStatusId"))
timekeepingStatusesExtractPath <- paste(baseExtractPath, "timekeepingStatuses.rds", sep = "/")
saveRDS(timekeeping_statuses_df, file = timekeepingStatusesExtractPath)

# COMMAND ----------

# MAGIC %fs ls "/FileStore/Rextracts"