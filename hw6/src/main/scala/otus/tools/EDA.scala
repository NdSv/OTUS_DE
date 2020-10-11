package otus.tools

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, mean}

class EDA {

  def show(data: org.apache.spark.sql.DataFrame): Unit = {

    // Initialize Spark Session
    val spark = SparkSession.builder().getOrCreate()

    // Show schema
    data.printSchema

    // Class Balance
    val classBalance = data.select(mean("target")).collect()(0)
    println(f"Class Balance: ${classBalance}")

    // Data Description
    println("Data Description")
    data.describe().show()

    val negative = data
      .filter(col("target")===lit(0))
      .describe()
      .withColumn("target", lit(0))

    val positive = data
      .filter(col("target")===lit(1))
      .describe()
      .withColumn("target", lit(1))

    println("Description By Class")
    negative.union(positive).show()
  }
}
