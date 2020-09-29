name := "hw2"

version := "0.1"

scalaVersion := "2.11.9"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
