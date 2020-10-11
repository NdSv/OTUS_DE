package otus.tools

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap

class RFFeatureSelector {

  def select(data: org.apache.spark.sql.DataFrame,
             featureCols: Array[String],
             labelCol: String = "target",
             threshold: Double = 0.01,
             verbose: Boolean = true): Array[String] = {

    val assemblerRF = new VectorAssembler()
    val paramAssemblerRF = ParamMap(
      assemblerRF.inputCols -> featureCols,
      assemblerRF.outputCol -> "features"
    )
    val rf = new RandomForestClassifier()
      .setLabelCol(labelCol)
      .setMaxDepth(4)
      .setNumTrees(100)
      .setSeed(42)

    val datasetRF = assemblerRF.transform(data, paramAssemblerRF)

    val featureImportances = rf
      .fit(datasetRF)
      .featureImportances
      .toDense.toArray.toList
      .zip(featureCols)
      .sortBy(-_._1)
      .map { case (imp, feature) => (feature, imp) }

    if (verbose) println(featureImportances.mkString(sep = "\n"))

    val featuresSelected = featureImportances
      .filter(_._2 > 0.01)
      .map { case (feature, _) => feature }
      .toArray

    if (verbose) println("Selected: ", featuresSelected.mkString(", "))

    featuresSelected

  }
}
