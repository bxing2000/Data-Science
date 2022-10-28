# Databricks notebook source
# MAGIC %run ./config_sandbox

# COMMAND ----------

library(dplyr)
library(forcats)
library(lubridate)
library(ggplot2)

#add factors to represent enum'd values
factor_PaymentCategory <- factor( 
  c(0, 1, 2, 3), 
  labels = c("Normal", "Promo", "Limited", "Demo")
)

factor_BillingIntegration <- factor(
  c(0, 1, 2, 3, 4, 5, 6, 7),
  labels = c("No Integration", "AppDirect", "Odin", "ATT3PP", "BellBSA", "AttSynchronoss", "SprintNOLWS", "IngramMicro")
)

#BillingFrequencyInterval 1 / "Not Applicable" == "Non Billable"
factor_BillingFrequencyUnit <- factor(
  c(1, 2, 3), labels = c("NotApplicable", "Daily", "Monthly")
)

#grab extracts
accounts <- readRDS("/dbfs/FileStore/Rextracts/All_Accounts.rds")
companies <- readRDS("/dbfs/FileStore/Rextracts/All_Companies.rds")
licenses <- readRDS("/dbfs/FileStore/Rextracts/All_Licenses.rds")
payments <- readRDS("/dbfs/FileStore/Rextracts/All_Payments.rds")

# COMMAND ----------

display(payments %>% head(50))

# COMMAND ----------

#pull up billable accounts from payment table and keep a subset
billable_accounts <- payments %>%
  filter(Billable == TRUE) %>%
  select(AccountId, Category, BillingIntegration, BillingFrequencyInterval)

#get account ids for billable accounts
billable_account_ids <- billable_accounts %>%
  select(AccountId)

#write out summary of billable accounts
billable_accounts %>% head(50)

# COMMAND ----------

#next get non-test accounts from accounts table
non_test_accounts <- accounts %>% 
  filter(IsTest == FALSE)

non_test_account_ids <- non_test_accounts %>% 
  select(AccountId)

#write out a few non-test accounts
non_test_accounts %>% select(AccountId, AccountName) %>% head(50)

# COMMAND ----------

#append non-test and billable account ids and unique() them to remove dupes
combined_account_ids <- rbind(non_test_account_ids, billable_account_ids) %>% 
  unique()

#get companies created in 2018, 2019, and 2020.  get year created as
#a factor so it's easy to use as a grouping variable, and inner_join
#with combined_account_ids to filter out nonbillable accounts.  after
#that's done, compute days from createdUTC to setup completion time.
billable_companies_2018_later <- companies %>% 
  filter(Created >= as.Date("2018-01-01")) %>%
  inner_join(combined_account_ids, by = "AccountId") %>%
  mutate(
    YearCreated = as.factor(year(Created)),
    DaysUtc = round(as.numeric(SetupCompletionTime - CreatedUtc)/(3600.0 * 24.0))
  )

billable_companies_2018_later %>% select(CompanyId, AccountId, CompanyName, CreatedUtc, SetupCompletionTime, DaysUtc) %>% head(50)

# COMMAND ----------

#now get companies whose setup completion time is NA and summarize
companies_2018_setup_not_completed <- billable_companies_2018_later %>%
  filter(is.na(SetupCompletionTime)) %>%
  select(AccountId, CompanyId, CompanyName, CreatedUtc, SetupCompletionTime, YearCreated)
companies_2018_setup_not_completed %>% head(20)
numNotCompleted <- nrow(companies_2018_setup_not_completed)
print(paste(numNotCompleted, " companies with a NA setup completion date"))

# COMMAND ----------

#now get companies whose setup completion time is NOT NA and summarize
companies_2018_setup_completed <- billable_companies_2018_later %>%
  filter(!is.na(SetupCompletionTime)) %>%
  select(AccountId, CompanyId, CompanyName, CreatedUtc, SetupCompletionTime, YearCreated, DaysUtc)
companies_2018_setup_completed %>% head(50)

# COMMAND ----------

options(width=200)

#summarise to get min and max days
companies_2018_setup_completed %>%
  summarise(min_days = min(DaysUtc), max_days = max(DaysUtc))

# COMMAND ----------

#display table of frequencies with ranges 1-5, 6-10, etc.  they're displayed as "(1,6]" as an ASCII shorthand for "starts with 1, less than 6"
table(cut(companies_2018_setup_completed$DaysUtc, seq(0, 800, by = 5)))

# COMMAND ----------

companies_2018_setup_completed %>%
  ggplot(aes(x = DaysUtc)) +
  labs(title = "Frequency of Occurrence by Year Company Created") +
  geom_histogram(binwidth = 5) +
  scale_x_continuous(name = "Days from Company Creation to Setup Completion", breaks = seq(0,800,by = 50)) +
  scale_y_continuous(name = "Frequency") +
  facet_grid(YearCreated ~ .)

# COMMAND ----------

#note - one more step to take would be to pull out the companies where the registration happens in the first range (1-5 days) and rerun the analysis.
laggards_2018_df <- companies_2018_setup_completed %>%
  filter(DaysUtc > 5)
laggards_2018_df %>%
  ggplot(aes(x = DaysUtc)) +
  labs(title = "Frequency of Occurrence by Year Company Created if >5 days for setup completion") +
  geom_histogram(binwidth = 5) +
  scale_x_continuous(name = "Days from Company Creation to Setup Completion", breaks = seq(0,800,by = 50)) +
  scale_y_continuous(name = "Frequency") +
  facet_grid(YearCreated ~ .)

# COMMAND ----------

#note - one more step to take would be to pull out the companies where the registration happens in the first range (1-5 days) and rerun the analysis.
ultra_laggards_2018_df <- companies_2018_setup_completed %>%
  filter(DaysUtc > 25)
ultra_laggards_2018_df %>%
  ggplot(aes(x = DaysUtc)) +
  labs(title = "Frequency of Occurrence by Year Company Created if >25 days for setup completion") +
  geom_histogram(binwidth = 5) +
  scale_x_continuous(name = "Days from Company Creation to Setup Completion", breaks = seq(0,800,by = 50)) +
  scale_y_continuous(name = "Frequency") +
  facet_grid(YearCreated ~ .)