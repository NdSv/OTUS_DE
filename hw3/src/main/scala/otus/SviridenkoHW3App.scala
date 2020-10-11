package otus

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, collect_list, count, mean, row_number, split, sort_array, struct, udf}

object SviridenkoHW3App extends App {

  // Initialize Spark Session
  val spark = SparkSession.builder().getOrCreate()
  val sc = SparkContext.getOrCreate()
  import spark.implicits._

  // Load Data
  val crimesDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("hw3/crime.csv")
    .na.fill(Map("DISTRICT" -> "Unknown"))

  val offenseCodesDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("hw3/offense_codes.csv")
    .withColumn("CRIME_TYPE",
      split($"NAME", " - ").getItem(0))

  // Task 1
  val task1 = crimesDF
    .groupBy($"DISTRICT")
    .agg(count($"INCIDENT_NUMBER").alias("CRIMES_TOTAL"))

  // Task 2
  val quantTmp = crimesDF
    .groupBy($"DISTRICT", $"YEAR", $"MONTH")
    .agg(count($"INCIDENT_NUMBER").alias("NUMBER_OF_INCIDENTS"))

  quantTmp.createOrReplaceTempView("quantTmp")

  val task2 = spark.sql("select DISTRICT, percentile_approx(NUMBER_OF_INCIDENTS,0.5) as CRIMES_MONTHLY " +
    "from quantTmp group by DISTRICT")

  // Task 3
  val w = Window
    .partitionBy($"DISTRICT")
    .orderBy($"CRIME_TYPE_FREQUENCY".desc)

  val getCrimes = udf { crimeRanks: Seq[Row] =>
    crimeRanks
      .sortBy{case Row(rank: Int, crime: String) => rank}
      .map{ case Row(_: Int, crime: String) => crime }
      .mkString(sep=", ")
  }

  val task3 = crimesDF.as("crimes")
    .join(broadcast(offenseCodesDF).as("codes"),
      ($"crimes.OFFENSE_CODE" === $"codes.CODE"))
    .groupBy($"DISTRICT", $"CRIME_TYPE")
    .agg(count($"INCIDENT_NUMBER").alias("CRIME_TYPE_FREQUENCY"))
    .withColumn("CRIME_TYPE_RANK", row_number.over(w))
    .filter($"CRIME_TYPE_RANK" <= 3)
    .withColumn("CRIME_TYPE_RANK", struct($"CRIME_TYPE_RANK", $"CRIME_TYPE"))
    .groupBy($"DISTRICT")
    .agg(collect_list($"CRIME_TYPE_RANK").alias("CRIME_TYPE_RANK"))
    .withColumn("FREQUENT_CRIME_TYPES", getCrimes($"CRIME_TYPE_RANK"))
    .drop($"CRIME_TYPE_RANK")

  // Task 4-5
  val task45 = crimesDF
    .groupBy($"DISTRICT")
    .agg(mean($"Lat").alias("LAT"),
         mean($"Long").alias("LNG"))

  val result = task1
    .join(task2, "DISTRICT")
    .join(task3, "DISTRICT")
    .join(task45, "DISTRICT")

  result
    .sort("DISTRICT")
    .coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("hw3/result.csv")

}
