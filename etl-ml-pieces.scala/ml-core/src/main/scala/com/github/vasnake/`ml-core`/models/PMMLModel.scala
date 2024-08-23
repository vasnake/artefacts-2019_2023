/** Created by vasnake@gmail.com on 2024-07-12
  */
package com.github.vasnake.`ml-core`.models

import scala.io.Source

import com.github.vasnake.`ml-core`.models.interface.PMMLEstimator
import org.pmml4s.model.{ Model => PMMLModel }

/** Parameters `fileName` and `pmmlDump` are mutually exclusive.
  * Parameter `pmmlDump` have a priority, if not empty, estimator will be loaded from it.
  */
case class PMMLEstimatorConfig(
  featuresLength: Int,
  predictLength: Int,
  fileName: String = "",
  pmmlDump: String = ""
)

case class SGDClassifier(config: PMMLEstimatorConfig) extends PMMLEstimator { // TODO: rename to PMMLModel

  // TODO: make it `lazy val estimator` and get rid of `ready` variable
  private var ready: Boolean = false
  // TODO: pull out estimator+slicer to pluggable class, pmml-lib-object-wrapper
  private var estimator: PMMLModel = _
  private var featuresSelector: Slicer = _

  override def init(): Unit = if (!ready) {
    val model =
      if (config.pmmlDump.nonEmpty) PMMLModel.fromString(config.pmmlDump)
      else if (config.fileName.nonEmpty) PMMLModel(Source.fromFile(config.fileName))
      else sys.error("either file or dump must be specified")

    require(
      model.numClasses == config.predictLength &&
      (model.isClassification || model.isRegression) &&
      model.inputFields.length <= config.featuresLength, // input could be sparsed, ergo `<=`
      s"""SGDClassifier: loaded pmml model properties not matched with declared config:
         | ${model} numClasses ${model.numClasses}, inputNames ${model.inputFields.length} ${model.inputNames.mkString(",")},
         | clsf|regr (${model.isClassification}| ${model.isRegression});
         | ${config} """.stripMargin
    )

    estimator = model
    // x1, x2, ...
    featuresSelector = Slicer(estimator.inputFields.map(fld => fld.name.drop(1).toInt - 1))
    ready = true
  }

  override def predict(features: Array[Double]): Array[Double] = {
    require(ready, "PMML estimator not initialized")
    require(
      features.length > 0 && features.length == config.featuresLength,
      s"wrong features vector size, features.length: ${features.length}, config.featuresLength: ${config.featuresLength}"
    )

    val estimation = estimator.predict(featuresSelector.transform(features))
    require(
      estimation.length == config.predictLength,
      s"wrong predicted vector size, result.length: ${estimation.length}, config.predictLength: ${config.predictLength}"
    )

    // this conversion takes less than 2% of job time
    val predictResult = new Array[Double](config.predictLength)
    var idx: Int = 0
    estimation.foreach { x =>
      x match {
        case a: Double => predictResult(idx) = a
        case b: Float => predictResult(idx) = b.toDouble
        case c: Int => predictResult(idx) = c.toDouble
        case _ =>
          sys.error(
            s"SGDClassifier.predict, unexpected prediction value type in result: '${estimation.mkString(";")}'"
          )
      }

      idx += 1
    }

    predictResult
  }
}
