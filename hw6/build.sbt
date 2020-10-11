name := "hw6"

version := "0.1"

scalaVersion := "2.11.9"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided",
  "ml.dmlc" %% "xgboost4j" % "1.1.1",
  "ml.dmlc" %% "xgboost4j-spark" % "1.1.1"

)
