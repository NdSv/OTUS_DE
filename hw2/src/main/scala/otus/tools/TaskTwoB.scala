package otus.tools

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object TaskTwoB {

  def maxDiff(data: RDD[RowFormat]): RDD[(String, Double)] = {

    val rdd_init = data.map(el => (el.name, DateTime.parse(el.date)) -> el.close)
    val rdd_shift = data.map(el =>
      (el.name, DateTime.parse(el.date).plusDays(1)) -> el.close)

    rdd_init
      .join(rdd_shift).mapValues(v => v._1 / (v._2 - 1))
      .map{case (k, v) => (k._1 -> v)}
      .reduceByKey(math.max)
      .sortBy(_._2, ascending = false)
    }
}