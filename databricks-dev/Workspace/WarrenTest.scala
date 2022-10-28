// Databricks notebook source
val appID = "9075dcf2-ef65-413a-9a80-082c9a40b4b2"
val secret = "b_]QQr9id-cVMI?Sj9Mza8Uv6m2BMmT]"
val tenantID = "1a5d5bac-c4df-4a2c-9374-267fcff8eead"

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "9075dcf2-ef65-413a-9a80-082c9a40b4b2")
spark.conf.set("fs.azure.account.oauth2.client.secret", "b_]QQr9id-cVMI?Sj9Mza8Uv6m2BMmT]")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

// COMMAND ----------

val storageAccountName = "warrentest"
val appID = "9075dcf2-ef65-413a-9a80-082c9a40b4b2"
val secret = "b_]QQr9id-cVMI?Sj9Mza8Uv6m2BMmT]"
val fileSystemName = "tutorial"
val tenantID = "1a5d5bac-c4df-4a2c-9374-267fcff8eead"

spark.conf.set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storageAccountName + ".dfs.core.windows.net", "" + appID + "")
spark.conf.set("fs.azure.account.oauth2.client.secret." + storageAccountName + ".dfs.core.windows.net", "" + secret + "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantID + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://" + fileSystemName  + "@" + storageAccountName + ".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://" + "tutorial" + "@" + "warrentest" + ".dfs.core.windows.net/")

// COMMAND ----------

val df = spark.read.json("abfss://" + "tutorial" + "@" + "warrentest" + ".dfs.core.windows.net/small_radio_json.json")

// COMMAND ----------

df.Show()

// COMMAND ----------

