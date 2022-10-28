// Databricks notebook source
// MAGIC %md
// MAGIC # Aggregations and JOINs
// MAGIC Apache Spark&trade; and Databricks&reg; allow you to create on-the-fly data lakes.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Basic Aggregations
// MAGIC 
// MAGIC Using <a "https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions" target="_blank">built-in Spark functions</a>, you can aggregate data in various ways. 
// MAGIC 
// MAGIC Run the cell below to compute the average of all salaries in the people DataFrame.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> By default, you get a floating point value.

// COMMAND ----------

val peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")

// COMMAND ----------

import org.apache.spark.sql.functions.avg
val avgSalaryDF = peopleDF.select(avg($"salary") as "averageSalary")

// COMMAND ----------

// MAGIC %md
// MAGIC Convert that value to an integer using the `round()` function. See
// MAGIC <a href "https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" class="text-info">the documentation for <tt>round()</tt></a>
// MAGIC for more details.

// COMMAND ----------

import org.apache.spark.sql.functions.round
val roundedAvgSalaryDF = avgSalaryDF.select(round($"averageSalary") as "roundedAverageSalary")

roundedAvgSalaryDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the average salary, what are the maximum and minimum salaries?

// COMMAND ----------

import org.apache.spark.sql.functions.{min, max}
val salaryDF = peopleDF.select(max($"salary") as "max", min($"salary") as "min", round(avg($"salary")) as "averageSalary")

salaryDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Joining Two Data Sets
// MAGIC 
// MAGIC Correlate the data in two data sets using a DataFrame join. 
// MAGIC 
// MAGIC The `people` data set has 10 million names in it. 
// MAGIC 
// MAGIC > How many of the first names appear in Social Security data files? 
// MAGIC 
// MAGIC To find out, use the Social Security data set with first name popularity data from the United States Social Security Administration. 
// MAGIC 
// MAGIC For every year from 1880 to 2014, `dbfs:/mnt/training/ssn/names-1880-2016.parquet/` lists the first names of people born in that year, their gender, and the total number of people given that name. 
// MAGIC 
// MAGIC By joining the `people` data set with `names-1880-2016`, weed out the names that aren't represented in the Social Security data.
// MAGIC 
// MAGIC (In a real application, you might use a join like this to filter out bad data.)

// COMMAND ----------

// MAGIC %md
// MAGIC Start by taking a look at what the social security data set looks like. Each year is its own directory.

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/training/ssn/names-1880-2016.parquet/

// COMMAND ----------

// MAGIC %md
// MAGIC Let's load this file into a DataFrame and look at the data.

// COMMAND ----------

val ssaDF = spark.read.parquet("/mnt/training/ssn/names-1880-2016.parquet/")

display(ssaDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, with a quick count of distinct names, get an idea of how many distinct names there are in each of the tables.
// MAGIC 
// MAGIC DataFrames have a `distinct` method just for this purpose.

// COMMAND ----------

val peopleDistinctNamesDF = peopleDF.select($"firstName").distinct

// COMMAND ----------

peopleDistinctNamesDF.count()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC In preparation for the join, let's rename the `firstName` column to `ssaFirstName` in the Social Security DataFrame.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Question to ponder: why would we want to do this?

// COMMAND ----------

val ssaDistinctNamesDF = ssaDF.select($"firstName" as "ssaFirstName").distinct

// COMMAND ----------

// MAGIC %md
// MAGIC Count how many distinct names in the Social Security DataFrame.

// COMMAND ----------

ssaDistinctNamesDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Now join the two DataFrames.

// COMMAND ----------

val joinedDF = peopleDistinctNamesDF.join(ssaDistinctNamesDF, peopleDistinctNamesDF("firstName") === ssaDistinctNamesDF("ssaFirstName"))

// COMMAND ----------

// MAGIC %md
// MAGIC How many are there?

// COMMAND ----------

joinedDF.count()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC In the tables above, some of the salaries in the `peopleDF` DataFrame are negative. 
// MAGIC 
// MAGIC These salaries represent bad data. 
// MAGIC 
// MAGIC Your job is to convert all the negative salaries to positive ones, and then sort the top 20 people by their salary.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** See the Apache Spark documentation, <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in functions</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1
// MAGIC Create a DataFrame`PeopleWithFixedSalariesDF`, where all the negative salaries have been converted to positive numbers.

// COMMAND ----------

// TODO

import org.apache.spark.sql.functions.abs
val peopleWithFixedSalariesDF = // FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val belowZero = peopleWithFixedSalariesDF.filter($"salary" < "0").count()
dbTest("DF-L3-belowZero", 0, belowZero, "Expected 0 records to have a salary below zero, found " + belowZero)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2
// MAGIC 
// MAGIC Starting with the `peopleWithFixedSalariesDF` DataFrame, create another DataFrame called `PeopleWithFixedSalariesSortedDF` where:
// MAGIC 0. The data set has been reduced to the first 20 records.
// MAGIC 0. The records are sorted by the column `salary` in ascending order.

// COMMAND ----------

// TODO
val peopleWithFixedSalariesSortedDF = // FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val resultsDF = peopleWithFixedSalariesSortedDF.select($"salary")
dbTest("DF-L3-count", 20, resultsDF.count(), "Expected 20 records, found " + resultsDF.count())

println("Tests passed!")

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val results = resultsDF.collect()

dbTest("DF-L3-fixedSalaries-0", Row(2), results(0))
dbTest("DF-L3-fixedSalaries-1", Row(3), results(1))
dbTest("DF-L3-fixedSalaries-2", Row(4), results(2))

dbTest("DF-L3-fixedSalaries-10", Row(19), results(10))
dbTest("DF-L3-fixedSalaries-11", Row(19), results(11))
dbTest("DF-L3-fixedSalaries-12", Row(20), results(12))

dbTest("DF-L3-fixedSalaries-17", Row(28), results(17))
dbTest("DF-L3-fixedSalaries-18", Row(30), results(18)) 
dbTest("DF-L3-fixedSalaries-19", Row(31), results(19)) 

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC As a refinement, assume all salaries under $20,000 represent bad rows and filter them out.
// MAGIC 
// MAGIC Additionally, categorize each person's salary into $10K groups.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1
// MAGIC  Starting with the `peopleWithFixedSalariesDF` DataFrame, create a DataFrame called `peopleWithFixedSalaries20KDF` where:
// MAGIC 0. The data set excludes all records where salaries are below $20K.
// MAGIC 0. The data set includes a new column called `salary10k`, that should be the salary in groups of 10,000. For example:
// MAGIC   * A salary of 23,000 should report a value of "2".
// MAGIC   * A salary of 57,400 should report a value of "6".
// MAGIC   * A salary of 1,231,375 should report a value of "123".

// COMMAND ----------

// TODO
val peopleWithFixedSalaries20KDF = // FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val below20K = peopleWithFixedSalaries20KDF.filter($"salary" < "20000").count()
 
dbTest("DF-L3-count-salaries", 0, below20K)  

println("Tests passed!")

// COMMAND ----------

// TEST - Run this cell to test your solution.

import org.apache.spark.sql.functions.count
lazy val results = peopleWithFixedSalaries20KDF
  .select($"salary10k")
  .groupBy($"salary10k")
  .agg(count("*") as "total")
  .orderBy($"salary10k")
  .limit(5)
  .collect()

dbTest("DF-L3-countSalaries-0", Row(2,43792), results(0))
dbTest("DF-L3-countSalaries-1", Row(3,212630), results(1))
dbTest("DF-L3-countSalaries-2", Row(4,536536), results(2))
dbTest("DF-L3-countSalaries-3", Row(5,1055261), results(3))
dbTest("DF-L3-countSalaries-4", Row(6,1623248), results(4))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 3
// MAGIC 
// MAGIC Using the `peopleDF` DataFrame, count the number of females named Caren who were born before March 1980. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1
// MAGIC 
// MAGIC Starting with `peopleDF`, create a DataFrame called `carensDF` where:
// MAGIC 0. The result set has a single record.
// MAGIC 0. The data set has a single column named `total`.
// MAGIC 0. The result counts only 
// MAGIC   * Females (`gender`)
// MAGIC   * First Name is "Caren" (`firstName`)
// MAGIC   * Born before March 1980 (`birthDate`)

// COMMAND ----------

// TODO
val carensDF = // FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.

lazy val rows = carensDF.collect()
dbTest("DF-L3-carens-len", 1, rows.length, "Expected 1 recod, found " + rows.length)
dbTest("DF-L3-carens-total", Row(750), rows(0),  "Expected the total to be 750, found " + rows(0))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Review Questions
// MAGIC **Q:** What is the DataFrame equivalent of the SQL statement `SELECT count(*) AS total`  
// MAGIC **A:** ```.agg(count("*").alias("total"))```
// MAGIC 
// MAGIC **Q:** What is the DataFrame equivalent of the SQL statement 
// MAGIC ```SELECT firstName FROM PeopleDistinctNames INNER JOIN SSADistinctNames ON firstName = ssaFirstName```  
// MAGIC **A:** 
// MAGIC `peopleDistinctNamesDF.join(ssaDistinctNamesDF, peopleDistinctNamesDF($"firstName") === ssaDistinctNamesDF($"ssaFirstName"))`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC * Do the [Challenge Exercise]($./Optional/03-Joins-Aggregations).
// MAGIC * Start the next lesson, [Accessing Data]($./04-Accessing-Data).

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Topics & Resources
// MAGIC 
// MAGIC * <a href="http://spark.apache.org/docs/latest/sql-programming-guide.html" target="_blank">Spark SQL, DataFrames and Datasets Guide</a>
// MAGIC * <a href="https://databricks.com/blog/2017/08/31/cost-based-optimizer-in-apache-spark-2-2.html" target="_blank">Cost-based Optimizer in Apache Spark 2.2</a>