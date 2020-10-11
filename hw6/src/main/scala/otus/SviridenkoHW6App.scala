package otus

import otus.tools._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SviridenkoHW6App extends App {

  // Initialize Spark Session
  val spark = SparkSession.builder().getOrCreate()
  val sc = SparkContext.getOrCreate()

  // Load Data
  val data = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("hw6/heart.csv")

  val features = for (c <- data.columns
                      if c!="target") yield c

  // EDA
  println("==========EDA==========")
  new EDA().show(data)
  println

  // Select Features
  println("==========Feature Selection==========")
  val featuresSelected = new RFFeatureSelector().select(data, features)
  println

  // LogReg Model Selection
  println("==========Logistic Regression==========")
  val bestLogReg = new LogRegSelector().select(data, featuresSelected)
  println

  // XGBoost Model Selection
  println("==========XGBoost Classifier==========")
  val bestXGBoost = new XGBoostSelector().select(data, featuresSelected)
  println

}