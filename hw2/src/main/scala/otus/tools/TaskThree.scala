package otus.tools
import java.time.{LocalDate, Period}

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TaskThree {

  val path = "3.txt"
  val pearsonsCorrelation = new PearsonsCorrelation()

  def val2Array(value: Seq[(String, Double)], minDate: String, maxDate: String): Array[Double] = {

    val valueMap = value.toMap

    val start = LocalDate.parse(minDate)
    val end = LocalDate.parse(maxDate)
    val datesRange = (0L to (end.toEpochDay - start.toEpochDay))
      .map(days => start.plusDays(days))
      .map(_.toString)

    datesRange.map(date =>
      (date, if (value.map(_._1).contains(date)) valueMap(date) else -1.0 ))
      .map(_._2)
      .toArray
  }

  def computeCorr(id1: String, id2: String, id2ValueMap: Map[String, Array[Double]]): Double = {
    val v1 = id2ValueMap(id1)
    val v2 = id2ValueMap(id2)

    val (v1Filtered, v2Filtered) = v1.zip(v2).filter(el => el._1 >= 0 && el._2 >= 0).unzip
    pearsonsCorrelation.correlation(v1Filtered, v2Filtered)
  }

  def vecById(data: RDD[RowFormat]): Map[String, Array[Double]] = {

    val rdd = data.map(el => el.name -> (el.date, el.close))
    val minDate = data.map(_.date).min
    val maxDate = data.map(_.date).max

    rdd
      .groupByKey()
      .map{case (key, value) => (key, value.toSeq)}
      .map{case (key, value) => (key, val2Array(value, minDate, maxDate))}.collectAsMap().toMap
  }

  def maxCorr(data: RDD[RowFormat]): RDD[((String, String), Double)] = {

    val sc = SparkContext.getOrCreate()
    val id2ArrayMap = sc.broadcast(vecById(data))
    val rddUniqueIds = data.map(el => el.name).distinct()
    val rddIdPairs = rddUniqueIds
      .cartesian(rddUniqueIds)
      .filter(el => el._1 < el._2)

    rddIdPairs
      .mapPartitions(pairs => pairs.map(pair => pair -> computeCorr(pair._1, pair._2, id2ArrayMap.value)))
      .sortBy(_._2, ascending = false)
  }

}