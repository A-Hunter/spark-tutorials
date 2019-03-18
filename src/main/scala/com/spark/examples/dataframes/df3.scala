/**
  * Created by Ghazi Naceur on 29/11/2018.
  */

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\infos.csv")

df.filter("id > 100 AND salary > 3000.0").show()

// Collecting result in Scala Object
val result = df.filter("id > 100 AND salary > 3000.0").collect()

// Count result
val count = df.filter("id > 100 AND salary > 3000.0").count()

df.printSchema()

df.show()

// mean() is one of the aggregate functions : Taking the average/mean from any numerical column
df.groupBy("last_name").mean().show()
df.groupBy("last_name").max().show()
df.groupBy("last_name").min().show()
df.groupBy("last_name").sum().show()


df.select(countDistinct("salary")).show()
df.select(sumDistinct("salary")).show()
df.select(variance("salary")).show()
df.select(stddev("salary")).show()
df.select(collect_set("salary")).show()

// Order is ASC by default
df.orderBy("salary").show()

// Desc Order
df.orderBy($"salary".desc).show()