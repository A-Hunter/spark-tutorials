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

val rdd1 = sc.parallelize(List("Alice", "Bob", "Joe")).map(name => (name,1))
val rdd2 = sc.parallelize(List("John", "Alice", "Daniel")).map(name => (name,1))

rdd1.join(rdd2).collect() // common name between rdd1 and rdd2
rdd1.leftOuterJoin(rdd2).collect() // taking the left values and associating with commons
rdd1.rightOuterJoin(rdd2).collect() // taking the right values and associating with commons