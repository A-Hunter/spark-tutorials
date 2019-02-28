/**
  * Created by Ghazi Naceur on 28/11/2018.
  */

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\infos.csv")
df.printSchema()
//
// Creating a new column, that represents the addition of 2 existing columns (2 integers)
val df2 = df.withColumn("uid", df("id") + df("salary"))

df.printSchema()
df2.printSchema()
// Renaming the new column
df2.select(df2("uid").as("unique_id")).show()
//
// Filtering with scala
import spark.implicits._ // ==> This import allows us to use the "$" scala notation

// Filtering with scala
df.filter($"salary" > 3000.0).show()

// Filtering with SQL
df.filter("salary > 3000.0").show()
//
// Multiple filtering with scala
df.filter($"id" > 100 && $"salary" > 3000.0).show()

// Multiple filtering with SQL
df.filter("id > 100 AND salary > 3000.0").show()
//
// Collecting the result of filtering
val result = df.filter("id > 100 AND salary > 3000.0").collect()

// Counting result
val count = df.filter("id > 100 AND salary > 3000.0").count()

df.filter($"id" == 10).show() // Error cannot be applied
df.filter($"id" === 10).show() // Success
df.filter("id = 10").show() // Success

// Correlation between 2 columns
df.select(corr("id", "salary")).show()