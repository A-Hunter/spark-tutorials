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

// The simple way to create RDDs is to take an existing collection in the driver program
//    and to pass it to SparkContext's parallelize() method
// Associating to each word, "1" value
val wordPairsRDD = sc.parallelize(Array("A", "B", "C", "A", "A")).map(word => (word,1))
// 1 is an initial value
// The purpose is to aggregate values with the same keys (A, B and C)
// (_+_,_+_)  ===>((i, i1) => i + i1, (i, i1) => i + i1)
val data1 = wordPairsRDD.aggregateByKey(1)(_+_,_+_).collect() // 3(A) + 1 = 4
val data2 = wordPairsRDD.aggregateByKey(0)(_+_,_+_).collect() // 3(A) + 1 = 4
// We could calculate how many times a word did occur in an array of String