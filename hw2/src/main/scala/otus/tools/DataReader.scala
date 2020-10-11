package otus.tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object DataReader extends (() => RDD[RowFormat]) {

  val spark = SparkSession.builder().getOrCreate()
  val fileName = "hw2/all_stocks_5yr.csv"

  override def apply(): RDD[RowFormat] = {

    spark.sparkContext
      .textFile(fileName)  // original file
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }
      .map(line => line.split(",")) // split by ","
      .filter(!_.exists(_.isEmpty))
      .map(
        col => RowFormat(
          col(0),
          col(1).toDouble,
          col(2).toDouble,
          col(3).toDouble,
          col(4).toDouble,
          col(5).toDouble,
          col(6)
        )
      )
  }
}