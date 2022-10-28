# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from pyspark.sql import SparkSession
from pyspark.sql.column import _to_java_column, _to_seq, Column
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# %%
dbutils = DBUtils(spark)
dbutils.fs.ls("dbfs:/")


# %%
# passthrough security is not supported by databricks-connect: 
# https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough#known-limitations
# Companies = spark.read.format('delta').load("/mnt/sandboxes/SampleDataSets/All/Companies_All")


# %%
blob_account_name = "azureopendatastorage"
blob_container_name = "citydatacontainer"
blob_relative_path = "Safety/Release/city=Boston"
blob_sas_token = r"?st=2019-02-26T02%3A34%3A32Z&se=2119-02-27T02%3A34%3A00Z&sp=rl&sv=2018-03-28&sr=c&sig=XlJVWA7fMXCSxCKqJm8psMOh0W4h7cSYO28coRqF2fs%3D"


# %%
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
print('Remote blob path: ' + wasbs_path)


# %%
df = spark.read.parquet(wasbs_path)
print('Register the DataFrame as a SQL temporary view: source')
df.createOrReplaceTempView('source')
df.count()


# %%
resDf = spark.sql('SELECT * FROM source LIMIT 10')
resDf.show()


# %%
# File location and type
file_location = "/FileStore/tables/WA_Fn_UseC__Telco_Customer_Churn.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location)

df.show(5)


# %%



