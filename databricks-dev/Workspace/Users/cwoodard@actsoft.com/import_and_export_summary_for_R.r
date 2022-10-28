# Databricks notebook source
# MAGIC %run ./config_sandbox

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("/mnt/SandboxStorage/SampleDataSets/" ))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #in this cell, create a dataframe containing companyId and a boolean flag for churned from these:
# MAGIC 
# MAGIC #active companies: 387,16959,9149,585,2784,7279,471,18090,14469,12618,10767,13940,16676,17940,15123,8279,1833,17172,13064,17957,15121,8246,1371,18155,14403,17418,12848,7426,15353,3187,5873,2302,1715,17054,18245,8765,15988,8228,14972,17658,1782,6277,10146,11549,5153,15418,4087,14171,583,13186,2740,7277,17194,9078,16301,6690,14400,10111,319,15739,2905,17003,14665,517,598,15937,14565,17251,16185,10276,14813,8027,3838,12433,12739,1450,8194,10880,13168,17574,14969,9408,763,7507,234,4284,2433,13714,14680,6391,12954,1267,15640,17928,17441,16069,14655,15490,10547,7193,9481,17589,10208,16067,12009,15755,35275,8961,9796,8861,2067,15555,166,11447,18191,6852,16232,18439,2084,10548,13184,10935,15472,8728,17671,918,16249,5274,4787,3017,7910,14983,13299,17836,12414,11448,11835,7290,10942,12662,11727,16164,1529,8273,10430,3157,2620,17503,14230,6420,15850,17520,13231,18124,9529,1015,15338,7222,15330,6337,15767,8007,17379,16271,17991,7139,36445,701,9775,14231,18239,17254,16767,3585,12659,12478,1147,14766,15601,16305,18056,17171,816,2536,36510,4964,6684,18452,8841,16907,9278,13815,10511,4204,15783,1716,14361,15683,17832,6988,7336,10022,4071,14278,36005,10576,6476,15063,16327,14863,14376,10493,4105,1013,14501,9477,15765,17087,36476,8840,15228,195,6185,11476,2483,13327,15078,2244,6731,15326,15501,17252,9136,1732,17681,4797,3812,12407,15449,16203,10294,12830,1449,6334,4077,15358,275,13713,12260,2780,15762,6151,36473,2580,16466,2399,11423,13622,13580,9870,11192,17449,14184,18234,11092,515,13647,7688,14076,8523,13539,17497,10622,2993,16960,5498,14399,35241,11257,12977,9225,224,16927,12827,2381,2860,8232,7653,8001,7820,12357,14166,18216,16265,14712,11620,14743,13290,14206,8784,5480,11826,17198,16711,14504,13009,12653,9082,2513,14281,18150,910,2711,3983,7148,14858,6967,36404,7048,12776,18446,17917,7471,12206,18015,12991,16643,11619,17478,644,14263,16818,11875,15446,13626,1577,17882,3726,13287,15973,15444,4998,13941,10676,6131,4280,13660,1444,957,9552,7651,1692,17997,16146,11122,13352,7270,3610,287,11924,13725,6452,14560,11774,15426,13575,9517,8551,17146,9030,5409,7129,16690,14229,3783,10040,10419,4997,10369,17948,1195,36452,15691,14277,36402,12905,17261,15004,3194,3063,17030,5162,18294,12285,15458,12235,14921,16641,9368,36517,12889,2524,16491,17682,16797,7128,16029,1525,14657,5177,4690,7732,10549,4946,12656,7880,12773,13558,13639,14424,17110,10672,6077,1888,10689,3060,6631,16590,17862,185,15524,10102,17275,9415,16994,16101,15441,14077,12555,8853,6944,17696,2836,9051,7679,6307,17588,14373,6432,9118,3109,17555,10282,17992,14719,16926,12331,10480,7134,10299,2141,4696,7513,588,9183,3224,16356,16306,10884,1215,2537,36430,11132,16860,11438,16373,12273,16323,10364,8513,1240,15736,7497,8463,13000,16173,1588,9785,8363,5090,15486,12619,12438,5107,10429,7214,11751,9363,13900,12536,2040
# MAGIC 
# MAGIC #inactive companies: 16553,9280,15974,8214,1768,12651,4056,8949,2684,17057,7627,783,8891,2584,14090,17611,10717,5693,2957,13751,14717,13701,7394,16766,17609,15627,10197,1023,9618,1858,973,17676,486,3172,7709,3072,11180,336,2535,16989,7459,11996,12831,1419,13543,8013,12369,11882,1567,13733,14518,1336,13583,11732,401,18426,4888,13252,9948,7699,11749,8178,14922,16236,17071,13013,13369,2567,7889,2954,7847,1938,13211,914,15855,3202,17038,15187,6194,2890,11087,12938,3806,5963,10979,3219,4889,3038,2988,5674,16151,16020,14648,2707,8079,9045,10765,17153,14673,8408,1533,17309,15102,6507,13599,8177,498,2218,17070,13755,16839,13218,15425,12202,11796,8581,1250,14251,15167,17374,9127,6920,8721,6341,2152,1623,7482,1086,15671,7076,1167,7911,6945,1871,2837,11432,17291,18118,7572,17002,2935,14216,2623,1869,853,2167,10631,1151,4324,9746,1936,6960,2017,15903,9944,1430,15316,16282,13067,3985,11695,6273,364,11383,14150,10579,4191,17721,18158,4968,13034,381,5753,12497,11960,1116,5522,15868,13174,3084,348,704,14802,15150,6505,596,8175,9141,8256,8562,14950,17636,7190,17149,13578,21157,5289,6066,8629,1356,7257,4042,6241,463,6678,6191,1735,5785,10678,15215,12479,661,3305,11413,3297,16916,4967,11967,2835,10008,15032,12694,10445,2198,11230,784,15280,3470,2983,13866,16950,1163,982,974,11378,11247,8909,10718,10181,4759,3735,1486,6023,10560,15097,17833,12759,949,14966,8659,1428,6321,362,3048,15295,2998,11593,14758,16436,10527,1403,9511,8626,9461,3552,35238,14833,15718,10246,6188,5651,6973,13180,5022,3113,2453,12014,5974,3369,5089,8254,5387,18121,8641,5287,16568,2551,2014,14261,12410,11923,13014,7592,16535,9262,16972,7890,16964,6955,5104,5583,8138,16814,13112,14913,13062,17112,6087,7707,1319,3162,4971,913,1269,8492,9856,3062,5269,14343,7418,980,5517,12740,5954,7268,1846,8632,3079,699,9773,18187,2400,14079,6806,13194,1384,16665,2648,12209,797,3614,4318,12457,11970,13996,16284,14781,5301,7458,16045,14144,8235,7350,10392,7300,7169,15764,862,15795,12085,4812,16978,4102,3217,4052,8052,17969,35159,16060,1556,9622,4679,2828,18167,14944,18117,3126,12027,8325,11977,5581,15498,8117,14324,10753,15109,3068,861,3547,16150,12092,6250,17662,10389,16777,11753,3108,15711,9273,1513,1463,6348,9521,3612,10356,15728,3381,15091,17777,943,16413,3629,14910,17902,10223,3785,12859,15495,7735,1347,1695,11256,11206,17463,18429,1495,652,3338,17132,7165,15710,10288,8785,16016,16395,13172,10436,7132,9949,2197,12635,4875,2851,10553,4165,8602,6264,4015,2164,7536,5198,7834,36436,2891,2841,10239,6181,14247,8288,3693,17212,13908,6279,857,7164,14345,8080,1205,9800,13452,3493,1592,15122,12386,14462,10841,874,2196,13477,14841,693,1130,1080,6981,9138,14641,6402,9088,9567,13038,11187,6469,2411,11972,6419,16378,9990,2261,410,13542,9484,14427,9353,17592,17063,12039,13790,4310,13203,9980,11171,15310,16145,16493,7500,12385,6963,4096,35253,12641,8583,15806,9897,13070,5310,16591,17557,15219,9666,16847,6401,15344,11286,3178,1806,17632,8021,3078,16566,3028,16558,1177,2410,16995,17343,3326,18309,10020,5077,6797,7632,12169,4896,12648,12606,9912,4011,4003,8061,2508,2864,7401,17310,16781,3193,1342,3143,7549,12086,4813,5648,7978,5598,7928,6912,14622,7391,14797,15763,17483,11068,17325,11772,6829,3077,5407,341,12986,2977,36464,14250,5655,11391,10904,18085,9432,17490,5680,36002,4258,2894,16853,10902,5878,2786,3142,406,6134,12878,1110,2911,11935,5060,4662,5010,18142,16291,3646,6811,17563,13018,9266,6174,1100,14588,11852,12818,3076,340,4877,12985,8885,10555,1473,16843,18076,17678,13141,18157,10266,6776,8496,9860,12902,5273,6108,8265,5398,9935,1777,405,3976,10314,7934,12471,16529,10562,13206,8430,2521,3364,5207,3314,2471,3306,2777,3743,10479,16338,16694,12636,3106,11033,5611,6090,2875,18206,7362
# MAGIC 
# MAGIC # and use them to filter in our sampled companies

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # aggregate form submissions
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import dateutil
# MAGIC import datetime
# MAGIC from pyspark.sql.functions import  col, sum, count, countDistinct
# MAGIC 
# MAGIC order_statuses_all_df = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/All/OrderStatuses_All_2019").selectExpr("CompanyId", "Date(StartUtcTime) AS UsageDate", "UserId", "OrderStatusId").groupBy(["CompanyId", "UsageDate", "UserId"]).agg(
# MAGIC   countDistinct(col("OrderStatusId")).alias("OrderStatusCount")
# MAGIC )
# MAGIC 
# MAGIC #print("writing out order_statuses_all_df table")
# MAGIC #dbutils.fs.rm('/FileStore/tables/aggregated_order_statuses_all_df.parquet', True)
# MAGIC #order_statuses_all_df.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/aggregated_order_statuses_all_df.parquet')
# MAGIC 
# MAGIC print("Writing out order statuses CSV file")
# MAGIC dbutils.fs.rm('/FileStore/tables/agg_order_statuses_all.csv', True)
# MAGIC order_statuses_all_df.coalesce(1).write.csv('/FileStore/tables/agg_order_statuses_all.csv')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # aggregate form submissions
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import dateutil
# MAGIC import datetime
# MAGIC from pyspark.sql.functions import  col, sum, count, countDistinct
# MAGIC 
# MAGIC companies_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/All/Companies_All")
# MAGIC print("ROWS IN COMPANIES:", companies_DF.count())
# MAGIC #CompanyId, CompanyName, CreatedUtc, City, RegionCode, PostalCode, CountryCode, VerticalId, Tier
# MAGIC 
# MAGIC licenses_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/All/Licenses_All").selectExpr("CompanyId", "LicenseId")
# MAGIC 
# MAGIC verticals_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/All/Verticals_full_2019").selectExpr("VerticalId", "ParentVerticalId", "Name")
# MAGIC 
# MAGIC sampled_forms_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/ByCompany/FormHeaders_SampledByCompany_All").selectExpr(
# MAGIC   "CompanyId", "StartedByUserId as UserId", "Date(StartUtcTimeTag) AS UsageDate", "FormHeaderId"
# MAGIC ).groupBy(['CompanyId', 'UsageDate', 'UserId']).agg(
# MAGIC   count(col('FormHeaderId')).alias('ModuleFormsCount')
# MAGIC )
# MAGIC 
# MAGIC timekeeping_statuses_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/ByCompany/TimekeepingStatus_SampledByCompany_All").selectExpr(
# MAGIC   "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "StatusId"
# MAGIC ).groupBy(['CompanyId', 'UsageDate', 'UserId']).agg(
# MAGIC   count(col('StatusId')).alias('TimekeepingStatusCount')
# MAGIC )
# MAGIC #CompanyId, StartUtcTime, IsRoot, UserId
# MAGIC 
# MAGIC trips_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/ByCompany/Trips_SampledByCompany_All").selectExpr(
# MAGIC   "CompanyId", "UserId", "Date(StartUtcTime) AS UsageDate", "StartLat"
# MAGIC ).groupBy(['CompanyId', 'UsageDate', 'UserId']).agg(
# MAGIC   count(col('StartLat')).alias('TripCount')
# MAGIC )
# MAGIC #CompanyId, StartUtcTime, UserId
# MAGIC 
# MAGIC users_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/ByCompany/Users_SampledByCompany_All")
# MAGIC #CompanyId, UserId, IsActive, ActivationDate, DeactivationDate
# MAGIC 
# MAGIC orders_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/ByCompany/OrderHeaders_SampledByCompany_All").selectExpr(
# MAGIC   "CompanyId", "CreatedByUserId AS UserId", "Date(CreatedUTCTime) AS UsageDate", "OrderId"
# MAGIC ).groupBy(['CompanyId', 'UsageDate', 'UserId']).agg(
# MAGIC   count(col('OrderId')).alias('OrderHeaderCount')
# MAGIC )
# MAGIC #CompanyId, CreatedUTCTime, ModifiedUtc, CreatedByUserId, ModifiedByUserId
# MAGIC 
# MAGIC #since the companies dataset has *all* of the AT&T companies, we need to grab the distinct company id values from each of the
# MAGIC #usage datasets.
# MAGIC forms_company_ids = sampled_forms_DF.select("CompanyId").distinct()
# MAGIC timekeeping_company_ids = timekeeping_statuses_DF.select("CompanyId").distinct()
# MAGIC trips_company_ids = trips_DF.select("CompanyId").distinct()
# MAGIC orders_company_ids = orders_DF.select("CompanyId").distinct()
# MAGIC 
# MAGIC #union them all together to get a list of companies that appear in the usage datasets
# MAGIC union_of_ids = forms_company_ids.union(timekeeping_company_ids).union(trips_company_ids).union(orders_company_ids)
# MAGIC 
# MAGIC #and use an inner join (easier in Pandas than chasing down which Pyspark SQL syntax to use) to filter in just the
# MAGIC #companies that appear in the usage data.
# MAGIC filtered_companies_DF = companies_DF.join(union_of_ids, on=['CompanyId'], how='inner')
# MAGIC print("ROWS IN FILTERED COMPANIES:", filtered_companies_DF.count())
# MAGIC 
# MAGIC flitered_licenses_DF = licenses_DF.join(union_of_ids, on=['CompanyId'], how='inner')
# MAGIC 
# MAGIC filtered_users_DF = users_DF.join(union_of_ids, on=['CompanyId'], how='inner')
# MAGIC 
# MAGIC #now begin to write the datasets out into local storage so I can more easily get them in R without having to run the same notebook
# MAGIC #over and over.
# MAGIC 
# MAGIC print("writing out sampled_companies table")
# MAGIC dbutils.fs.rm('/FileStore/tables/sampled_companies.parquet', True)
# MAGIC filtered_companies_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/sampled_companies.parquet')
# MAGIC 
# MAGIC print("writing out sampled_company_users table")
# MAGIC dbutils.fs.rm('/FileStore/tables/sampled_company_users.parquet', True)
# MAGIC filtered_users_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/sampled_company_users.parquet')
# MAGIC 
# MAGIC #print("writing out licenses table")
# MAGIC #dbutils.fs.rm('/FileStore/tables/licenses.parquet', True)
# MAGIC #flitered_licenses_DF.write.partitionBy('LicenseId').format('parquet').save('/FileStore/tables/licenses.parquet')
# MAGIC 
# MAGIC #print("writing out verticals table")
# MAGIC #dbutils.fs.rm('/FileStore/tables/verticals.parquet', True)
# MAGIC #verticals_DF.write.partitionBy('VerticalId').format('parquet').save('/FileStore/tables/verticals.parquet')
# MAGIC 
# MAGIC print("writing out aggregated_forms table")
# MAGIC dbutils.fs.rm('/FileStore/tables/aggregated_forms.parquet', True)
# MAGIC sampled_forms_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/aggregated_forms.parquet')
# MAGIC 
# MAGIC print("writing out aggregated_order_headers table")
# MAGIC dbutils.fs.rm('/FileStore/tables/aggregated_order_headers.parquet', True)
# MAGIC orders_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/aggregated_order_headers.parquet')
# MAGIC 
# MAGIC print("writing out aggregated_timekeeping_statuses table")
# MAGIC dbutils.fs.rm('/FileStore/tables/aggregated_timekeeping_statuses.parquet', True)
# MAGIC timekeeping_statuses_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/aggregated_timekeeping_statuses.parquet')
# MAGIC 
# MAGIC print("writing out aggregated_trips table")
# MAGIC dbutils.fs.rm('/FileStore/tables/aggregated_trips.parquet', True)
# MAGIC trips_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/aggregated_trips.parquet')

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # aggregate form submissions
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import dateutil
# MAGIC import datetime
# MAGIC from pyspark.sql.functions import  col, sum, count, countDistinct
# MAGIC 
# MAGIC licenses_DF = spark.read.format('delta').load("/mnt/SandboxStorage/SampleDataSets/All/Licenses_All").selectExpr(
# MAGIC   "LicenseId", "CompanyId", "Date(OriginalActivationDate) AS ActivationDate", "Date(DeactDate) AS DeactivationDate"
# MAGIC )
# MAGIC print("ROWS", licenses_DF.count())
# MAGIC #print("writing out sampled_licenses table")
# MAGIC #dbutils.fs.rm('/FileStore/tables/sampled_licenses.parquet', True)
# MAGIC #licenses_DF.write.partitionBy('CompanyId').format('parquet').save('/FileStore/tables/sampled_licenses.parquet')