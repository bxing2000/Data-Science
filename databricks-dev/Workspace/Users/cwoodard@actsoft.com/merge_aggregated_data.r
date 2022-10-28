# Databricks notebook source
library(sparklyr)
library(dplyr)
library(tidyr)
library(lubridate)
if(!require("xts")){
    install.packages("xts")
    library(xts)
}
else {
  library(xts)
}

sc = spark_connect(method = "databricks")

#pull in forms parquet file
aggregated_forms_df <- spark_read_parquet(sc, "/FileStore/tables/aggregated_forms.parquet")
#bring them into R memory
collected_forms_df <- sdf_collect(aggregated_forms_df)

spark_disconnect(sc)

#next up - company 5788 is likely republic services.  we can pick that one company and start building a time series by adding up the
#number of submissions to get the daily count of submissions for this company
co_5788_forms_per_date_df <- collected_forms_df %>% filter(CompanyId == 5788) %>% group_by(UsageDate) %>% summarise(count = sum(ModuleFormsCount))

#now we can build a xts time series from it and start playing.
xts_5788_counts <- co_5788_forms_per_date_df$count
xts_5788_dates <-  co_5788_forms_per_date_df$UsageDate
xts_5788 <- xts(xts_5788_counts, order.by = xts_5788_dates)

#series periodicity
periodicity(xts_5788)
nmonths(xts_5788)
