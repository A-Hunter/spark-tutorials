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

// Map
val mapFile = dataRDD.map(line => (line, line.length))
mapFile.foreach(println)
println(mapFile.count())

// FlatMap
val flatMapFile = dataRDD.flatMap(line => line.split(" "))
flatMapFile.foreach(println)

// Union
val rdd1 = sc.parallelize(Seq((1, "jan", 2016), (3, "nov", 2014), (16, "feb", 2014)))
val rdd2 = sc.parallelize(Seq((5, "dec", 2014), (17, "sep", 2015), (1, "jan", 2016)))
val rdd3 = sc.parallelize(Seq((6, "dec", 2016), (16, "may", 2015)))

val rddUnion = rdd1.union(rdd2).union(rdd3)
rddUnion.foreach(println)

// Intersection
val rddIntersection = rdd1.intersection(rdd2)
rddIntersection.foreach(println)

// Distinct
val rdd4 = sc.parallelize(Seq((6, "dec", 2016), (16, "may", 2015), (6, "dec", 2016)))
val rddDistinct = rdd4.distinct()
rddDistinct.foreach(println)
println(rddDistinct.collect().mkString(","))

// GroupBy
val rdd5 = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('t',8),('p',5),('k',6)), 3)
val rddGroup = rdd5.groupByKey().collect()
rddGroup.foreach(println)

// ReduceBy
val rddReduce = rdd5.reduceByKey(_+_)
rddReduce.foreach(println)

// SortBy
val rddSort = rdd5.sortByKey()
rddSort.foreach(println)

// Join
val rdd6 = sc.parallelize(Array(('A',1),('B',2),('C',3)))
val rdd7 = sc.parallelize(Array(('A',4),('A',6),('B',7),('C',3),('C',8)))
val rddJoin = rdd6.join(rdd7)
rddJoin.foreach(println)