/**
  * Created by Ghazi Naceur on 08/04/2019
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

// Creating an rdd from a text file : Converting the dataset to rdd using the method rdd
val dataRDD = spark.read.textFile("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\some_text.txt").rdd

// Filtering
val mapFile = dataRDD.flatMap(line => line.split(" ")).filter(value => value == "Scala")
println(mapFile.count()) // counting lines that contain Scala

// Collecting
val rdd1 = sc.parallelize(Array(('A',1),('B',2),('C',3)))
val rdd2 = sc.parallelize(Array(('A',4),('A',6),('B',7),('C',3),('C',8)))
val rddJoin = rdd1.join(rdd2)
println(rddJoin.collect().mkString(","))

// Take : Selecting first ''n'' elements.
val rdd3 = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('t',8),('p',5),('k',6)), 3)
val rddGroup = rdd3.groupByKey().collect()
val twoRec = rddGroup.take(2)
twoRec.foreach(println)

// Top : Returning the top k (largest) elements from this RDD as defined by the specified
//    implicit Ordering[T] and maintains the ordering
val mapFile2 = dataRDD.map(line => (line, line.length))
val top = mapFile2.top(3)
top.foreach(println)

// CountByValue
val countRdd = dataRDD.map(line => (line, line.length)).countByValue()
countRdd.foreach(println)

// Reduce
val rdd4 = sc.parallelize(List(20,32,45,62,8,5))
val sumRdd = rdd4.reduce(_+_)
println(sumRdd)

// GroupByKey
val rdd5 = rdd3.groupByKey().collect()
rdd5.foreach(println)