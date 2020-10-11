package otus.tools

import ml.dmlc.xgboost4j.scala.spark.{TrackerConf, XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}


class XGBoostSelector {

  def select(data: org.apache.spark.sql.DataFrame,
            featureCols: Array[String],
            target: String = "target",
            verbose: Boolean = true): XGBoostClassificationModel = {

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val trackerConf = new TrackerConf(
      workerConnectionTimeout=10, trackerImpl = "scala"
    )
    val xgb = {new XGBoostClassifier(
      Map("verbosity" -> 0, "track_conf" -> trackerConf)
    )
      .setLabelCol(target)
      .setFeaturesCol(assembler.getOutputCol)
      .setPredictionCol("prediction")
      .setObjective("binary:logistic")
      .setMissing(0.toFloat)
      .setCheckpointPath("hw6/logs.txt")
      .setCheckpointInterval(-1)}
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(xgb.getLabelCol)
      .setPredictionCol(xgb.getPredictionCol)
      .setMetricName("accuracy")
    val paramGrid = new ParamGridBuilder()
      .addGrid(xgb.maxDepth, (3 to 7).toArray)
      .addGrid(xgb.numRound, Array(25, 50, 100, 200))
      .addGrid(xgb.eta, Array(0.05, 0.1, 0.2, 0.5))
      .addGrid(xgb.alpha, Array(0, 0.5, 1))
      .build()
    val cv = new CrossValidator()
      .setEstimator(xgb)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(1)

    val dataset = assembler.transform(data).select("target", "features")
    val cvModel = cv.fit(dataset)
    val bestModel = cvModel.bestModel.asInstanceOf[XGBoostClassificationModel]

    if (verbose) {
      println(f"Best Avg Accuracy: ${cvModel.avgMetrics.max}")
      println("Best Model Params: ")
      println(bestModel.extractParamMap())
    }
    bestModel
  }
}
