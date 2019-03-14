/**
  * Created by Ghazi Naceur on 14/03/2019
  * Email: ghazi.ennacer@gmail.com
  */

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV file, have Spark infer the data types.
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\Netflix_2011_2016.csv")

// What are the column names ?
df.columns

// What does the schema look like ?
df.printSchema()

// Print out the first 5 columns
df.head(5)

// Use describe() to learn about the DataFrame
df.describe().show()

// Create a new DataFrame with a column called HV Ratio that is the ratio of the High Price versus
// volume of stock traded for a day
val df2 = df.withColumn("HV Ratio", df("High") / df("Volume"))
df.columns
df2.show()

// What day had the peak High in Price ?
df.orderBy($"High".desc).show(1)

// What is the mean of the Close column ?
//    Inside the select, we pass the aggregate function : In this case, it's the mean function
df.select(mean("Close")).show()

// What is the max and the min of the Volume column ?
df.select(max("Volume")).show()
df.select(min("Volume")).show()

// For Scala/Spark $ Syntax
import spark.implicits._

// How many days was the Close lower than $600 ?
df.filter($"Close" < 600).count()

// We can use another method (Spark SQL)
df.filter("Close < 600").count()

// What the percentage of the time was the High greater than $500 ?
(df.filter($"High" > 500).count() / df.count()) * 100
// => When doing this division operation, you will obtain 0 , because it's a division between Integers
//    So you need to convert them to Double (by multiplying by 1.0) :
(df.filter($"High" > 500).count() * 1.0 / df.count()) * 100

// What is the Pearson correlation between High and Volume ?
df.select(corr("High", "Volume")).show()

// What is the max High per year ?
val yeardf = df.withColumn("Year", year(df("Date")))
val yearmaxs = yeardf.select($"Year", $"High").groupBy("Year").max()
yearmaxs.select($"Year", $"max(High)").show()
// we can order the result :
val result = yearmaxs.select($"Year", $"max(High)")
result.orderBy("Year").show()

// What is the average Close for each Calender Month ?
val monthdf = df.withColumn("Month", month(df("Date")))
val monthavg = monthdf.select($"Month", $"Close").groupBy("Month").avg() // we can use .mean() instead of .avg()
monthavg.select($"Month", $"avg(Close)").show()
// We can order the result :
val result2 = monthavg.select($"Month", $"avg(Close)")
result2.orderBy("Month").show()