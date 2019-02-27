

/**
  * Created by Ghazi Naceur on 26/11/2018.
  */
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header","true").option("inferSchema","true").csv("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\MOCK_DATA.csv")
// option("header","true") ==> to consider the first line as a header
// option("inferSchema","true") ==> to cast fields from String to its real type : int, double ...

df.head(5)

// Display the 5 first lines
for(line <- df.head(5)){
  println(line)
}

// Display the columns names from the header
df.columns

// Display some statistics from the file in a table : min, max, stddev, count and mean
df.describe().show()

// Select a specific column
df.select("first_name").show()

df.select($"first_name", $"last_name").show()

// Add a new column
val df2 = df.withColumn("uid", df("id")+df("ip_address"))

df.printSchema()
df2.printSchema()

df2.select(df2("uid").as("unique_id")).show()
