/**
  * Created by Ghazi Naceur on 18/03/2019
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

// The simple way to create RDDs is to take an existing collection in the driver program
//    and to pass it to SparkContext's parallelize() method
val data = sc.parallelize(Seq(("maths",52),("english",75),("science",82),("computer", 65),("maths", 85)))

val sorted = data.sortByKey()
sorted.foreach(println)

val spark = SparkSession.builder().appName("AvgAnsTime").master("local").getOrCreate()

// Creating an rdd from a text file : Converting the dataset to rdd using the method rdd
val dataRDD = spark.read.textFile("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\some_text.txt").rdd
dataRDD.foreach(println)

// Creating an rdd from an existing rdd
val words = sc.parallelize(Seq("This", "is", "a", "sequence", "of", "words"))
val wordPair = words.map(w => (w.charAt(0), w))
wordPair.foreach(println)