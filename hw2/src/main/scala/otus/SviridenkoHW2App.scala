package otus

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import tools._

object SviridenkoHW2App extends App {

  // Initialize Spark Session
  val spark = SparkSession.builder().getOrCreate()
  val sc = spark.sparkContext

  val data = DataReader()

  val twoA = "2a - " + TaskTwoA.maxDiff(data).take(3).map(_._1).mkString(sep=", ")
  val twoB = "2b - " + TaskTwoB.maxDiff(data).take(3).map(_._1).mkString(sep=", ")
  val three = "3 - " + TaskThree.maxCorr(data).take(3).map(_._1).mkString(sep=", ")

  println(twoA)
  println(twoB)
  println(three)

  sc
    .parallelize(List(twoA, twoB, three))
    .coalesce(1)
    .saveAsTextFile("hw2/result.txt")

}