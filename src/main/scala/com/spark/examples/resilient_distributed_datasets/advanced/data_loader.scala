/**
  * Created by Ghazi Naceur on 22/04/2019
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

val spark = SparkSession.builder().appName("new").master("local").getOrCreate()

val lines = spark.read.textFile("D:\\github-projects\\spark-tutorials\\src\\main\\resources\\employees.txt").rdd
lines.map(_.split(" ")).map(attributes => (attributes(0), attributes(1).trim.toInt)).sortByKey().saveAsTextFile("employees_output")