package otus

import tools.{DataReader, TaskTwoA, TaskTwoB, TaskThree}

object SviridenkoHW2App extends App {

  val data = DataReader()

  // TaskOne.maxDiff(data). //saveAsTextFile("2a.txt")
  // TaskTwoA.maxDiff(data).saveAsTextFile("2a.txt")
  // TaskTwoB.maxDiff(data).take(10).foreach(println)

  //TaskThree.vecById(data).take(10).foreach(x => {for (i <- 0 to 10 ) print(x._2(i))})
  TaskThree.maxCorr(data).take(10).foreach(println)
}