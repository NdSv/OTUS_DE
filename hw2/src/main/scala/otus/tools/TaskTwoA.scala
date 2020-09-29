package otus.tools

import org.apache.spark.rdd.RDD

case class MinMax(min: Double,
                  max: Double
                 )

object TaskTwoA {

  val path = "2a.txt"
  val initialCount = MinMax(min=.0, max=.0)
  val localMinMax = (acc: MinMax, row: RowFormat) => {
    MinMax(min = if (acc.min < row.low) acc.min else row.low,
           max = if (acc.max > row.high) acc.max else row.high)
  }
  val globalMinMax = (acc1: MinMax, acc2: MinMax) => {
    MinMax(min = if (acc1.min < acc2.min) acc1.min else acc2.min,
           max = if (acc1.max > acc2.max) acc1.max else acc2.max)
  }

  def maxDiff(data: RDD[RowFormat], topN: Int = 3): RDD[(String, Double)] = {
    data
      .map(el => el.name -> el)
      .aggregateByKey(initialCount)(localMinMax, globalMinMax)
      .mapValues(v => v.max - v.min)
      .sortBy(_._2, ascending = false)
      .zipWithIndex()
      .filter(el => el._2 < topN)
      .map(_._1)
    }
}