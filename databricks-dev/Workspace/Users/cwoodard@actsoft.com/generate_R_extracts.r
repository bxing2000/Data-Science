# Databricks notebook source
# MAGIC %run ./config_sandbox

# COMMAND ----------

baseExtractPath <- "/FileStore/Rextracts"
source_data_path <- "/mnt/SandboxStorage/SampleDataSets/All/"
dbutils.fs.ls(baseExtractPath)


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

companiesExtractPath <- paste(baseExtractPath, "All_Companies.rds", sep = "/")
print(companiesExtractPath)
all_companies_df <- as.data.frame(
    read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Companies_All", source = "delta"))
str(all_companies_df)
#saveRDS(all_companies_df, file = companiesExtractPath)

# COMMAND ----------

all_accounts_df <- as.data.frame(
    read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Accounts_All", source = "delta"))
str(all_accounts_df)
accountsExtractPath <- paste(baseExtractPath, "All_Accounts.rds", sep = "/")
saveRDS(all_accounts_df, file = accountsExtractPath)

# COMMAND ----------

all_payments_df <- as.data.frame(
    read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/PaymentInfo_All", source = "delta"))
str(all_payments_df)
paymentsExtractPath <- paste(baseExtractPath, "All_Payments.rds", sep = "/")
saveRDS(all_payments_df, file = paymentsExtractPath)

# COMMAND ----------

#read from the Licenses_All dataset and just bring in the company id, original activation date and deactivation date.  we're not playing with the Tier or LicenseId just yet; we really
#just need to know which companies are active and which ones are not (have churned)
adf <- as.data.frame(read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Licenses_All/", source = "delta"))
str(adf)
licensesExtractPath <- paste(baseExtractPath, "All_Licenses.rds", sep = "/")
saveRDS(adf, file = licensesExtractPath)

# COMMAND ----------

#load verticals
all_verticals_df <- as.data.frame(
read.df(path = "/mnt/SandboxStorage/SampleDataSets/All/Verticals_full_2019/", source = "delta"))
str(all_verticals_df)
all_verticals_path <- paste(baseExtractPath, "AllVerticals.rds", sep = "/")
saveRDS(all_verticals_df, file = all_verticals_path)

# COMMAND ----------

users_df <- as.data.frame(SparkR::selectExpr(
  read.df(path = "/mnt/SandboxStorage/SampleDataSets/ByCompany/Users_SampledByCompany_All", source = "delta"),
  "CompanyId", "UserId", "Date(CreatedUtc) AS Created", "Date(ModifiedUtc) AS LastModified", "Deleted", "IsActive", "Date(ActivationDate) AS ActivationDate", "Date(DeactivationDate) AS DeactivationDate"))
str(users_df)
usersExtractPath <- "/dbfs/FileStore/Rextracts/Users.rds"
saveRDS(users_df, file = usersExtractPath)