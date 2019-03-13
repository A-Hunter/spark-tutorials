/**
  * Created by Ghazi Naceur on 13/03/2019
  * Email: ghazi.ennacer@gmail.com
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import spark.implicits._

val spark = SparkSession.builder().getOrCreate()

val customSchema = StructType(Array(
  StructField("id",StringType, true),
  StructField("firstname", StringType, true),
  StructField("lastname", StringType, true),
  StructField("random_date", DateType, true)
))

val df = spark.read.option("header", "true").option("inferSchema", "true").option("dateFormat", "dd/MM/yyyy").schema(customSchema).csv("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\data_with_timestamps.csv")
// when setting the option("dateFormat", "dd/MM/yyyy"), you need to make sure that month symbol is "MM" and not "mm"

val dup = df
// show months
df.select(month(df("random_date"))).show()

// show years
df.select(year(df("random_date"))).show()

val df2 = dup.withColumn("year", year(df("random_date")))
df2.show()
val df2Avgs = df2.groupBy("year").mean()
// => This instruction returned a table with only 2 columns (year and avg(year)) caused by the groupBy operation
//    As a solution, we can perform a join with the old table (id, firstname, lastname, year(random_date)) and
//      the new table (year, avg(year)) using the column "year"

val firstSolution = df2.join(df2Avgs, "year")
firstSolution.show()

// The first solution can cause a problem when dealing a big mass of data, so it is preferable to use an aggregation
val secondSolution =  df2.groupBy("year").agg(mean("id"))
secondSolution.show()