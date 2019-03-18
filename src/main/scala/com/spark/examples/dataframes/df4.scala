/**
  * Created by Ghazi Naceur on 12/03/2019
  * Email: ghazi.ennacer@gmail.com
  */

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\missing_data.csv")

df.printSchema()

// Missing data is replaced with "null"
df.show()

// drop() : drops rows that contain "null"
df.na.drop().show()

// drop(2) : drops rows that contain less than 2 non null values => 1 non null value == column will be dropped
df.na.drop(2).show()

// fill(100) :  replaces "null" values in Int-type columns  with "100"
df.na.fill(100).show()

// fill(100) :  replaces "null" values in String-type columns  with "missing text"
df.na.fill("missing text").show()

// fills a specific column(s)
df.na.fill("missing firstname", Array("first_name")).show()

// using a generated data frame to continue filling the missing data
val df2 = df.na.fill("missing firstname", Array("first_name"))
df2.na.fill(100, Array("salary")).show()