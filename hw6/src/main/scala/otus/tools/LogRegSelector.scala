package otus.tools

import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}


class LogRegSelector {

  def select(data: org.apache.spark.sql.DataFrame,
            featureCols: Array[String],
            target: String = "target",
            verbose: Boolean = true): LogisticRegressionModel = {
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val normalizer = new Normalizer()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("featuresNormalized")
    val lr = new LogisticRegression()
      .setLabelCol(target)
      .setFeaturesCol(normalizer.getOutputCol)
      .setPredictionCol("prediction")
      .setMaxIter(100)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(lr.getLabelCol)
      .setPredictionCol(lr.getPredictionCol)
      .setMetricName("accuracy")
    val pipeline = new Pipeline()
      .setStages(Array(normalizer, lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, (-3 to 1).toArray.map(math.pow(10, _)))
      .addGrid(lr.elasticNetParam, Array(0, 0.2, 0.4, 0.6, 0.8))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2)

    val dataset = assembler.transform(data)
    val cvModel = cv.fit(dataset)
    val bestModel = cvModel.bestModel
      .asInstanceOf[PipelineModel].stages(1)
      .asInstanceOf[LogisticRegressionModel]

    if (verbose) {
      println(f"Best Avg Accuracy: ${cvModel.avgMetrics.max}")
      println("Best Model Params: ")
      println(bestModel.extractParamMap())
    }
    bestModel
  }
}
