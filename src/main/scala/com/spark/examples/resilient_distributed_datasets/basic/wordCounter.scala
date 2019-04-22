/**
  * Created by Ghazi Naceur on 12/04/2019
  * Email: ghazi.ennacer@gmail.com
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Closing the old context
sc.stop()

val conf = new SparkConf().setMaster("local").setAppName("MyApp")
// Creating a new context
val sc = new SparkContext(conf)

val spark = SparkSession.builder().appName("AvgAnsTime").master("local").getOrCreate()

val input = sc.textFile("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\some_text.txt")
val words = input.flatMap(x => x.split(" "))
val result = words.map(x => (x,1)).reduceByKey((x,y) => x + y).sortByKey()
result.collect()

// or

sc.textFile("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\some_text.txt").flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => (x + y)).sortByKey().collect()