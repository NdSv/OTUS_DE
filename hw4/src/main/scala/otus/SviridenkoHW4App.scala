package otus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SviridenkoHW4App extends App {

  // Initialize Spark Session
  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .option("checkpointLocation", "hw4/checkpoint")
    .start()

  query.awaitTermination()

}