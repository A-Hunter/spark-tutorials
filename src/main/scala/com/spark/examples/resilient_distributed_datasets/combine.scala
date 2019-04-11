/**
  * Created by Ghazi Naceur on 11/04/2019
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

val rdd1 = sc.parallelize(List(("A", 3),("A", 9),("A", 12),("A", 0),("A", 5),("B", 4),("B", 10),("B", 11),("B", 20),("B", 25),("C", 32),("C", 91),("C", 122),("C", 3),("C", 55)), 2)
val rdd2 = rdd1.combineByKey((v:Int) => v.toLong, (c: Long, v: Int) => c + v, (c1: Long, c2: Long) => c1 + c2)
rdd2.collect()

val rdd3 = sc.parallelize(List((1,2),(3,4),(3,6)))
rdd3.countByKey() // Map(1 -> 1, 3 -> 2)

rdd3.collectAsMap() //  Map(1 -> 2, 3 -> 6)

rdd3.lookup(3) // WrappedArray(4, 6)