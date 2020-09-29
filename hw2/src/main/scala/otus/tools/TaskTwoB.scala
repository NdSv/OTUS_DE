package otus.tools

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object TaskTwoB {

  val path = "2b.txt"

  def maxDiff(data: RDD[RowFormat], topN: Int = 3): RDD[((String, DateTime), Double)] = {

    val rdd_init = data.map(el => (el.name, DateTime.parse(el.date)) -> el.close)
    val rdd_shift = data.map(el =>
      (el.name, DateTime.parse(el.date).plusDays(1)) -> el.close)

    rdd_init
      .join(rdd_shift).mapValues(v => v._1 / (v._2 - 1))
      .sortBy(_._2, ascending = false)
      .zipWithIndex()
      .filter(el => el._2 < topN)
      .map(_._1)
    }
}