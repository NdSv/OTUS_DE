package otus

import tools._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{DataFrame, Dataset}


object SviridenkoHW4App extends App {

  // Initialize Spark Session
  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  // Checkpoint location
  val checkpointDir = "hw4/checkpoint"
  spark.conf
    .set("spark.sql.streaming.checkpointLocation", checkpointDir)

  // Load lines from nc -lk 9999 as DataFrame
  val df: DataFrame = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  val ds: Dataset[ClientData] = df
    .as[String]
    .map(ClientData.parse)

  // Generate statistics
  val aggData = ds
    .groupByKey(_.gender)
    .agg(typed.avg(_.age))

  // Output
  val query = aggData.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

}