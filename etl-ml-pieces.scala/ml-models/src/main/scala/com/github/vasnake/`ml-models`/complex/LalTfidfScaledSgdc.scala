/** Created by vasnake@gmail.com on 2024-07-12
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`etl-core`.GroupedFeatures
import com.github.vasnake.`ml-core`.models._

case class LalTfidfScaledSgdcModelConfig(
  imputerConfig: Array[Double],
  tfidfConfig: GroupedFeaturesTfidfTransformerConfig,
  scalerConfig: ScalerConfig,
  predictorWrapperConfig: PredictorWrapperConfig,
  predictorConfig: PMMLEstimatorConfig,
  equalizerConfig: SBGroupedTransformerConfig,
)

case class LalTfidfScaledSgdcModel // TODO: rename to LalTfidfScaledSgdc
(
  config: LalTfidfScaledSgdcModelConfig,
  groupedFeatures: GroupedFeatures,
  audienceName: String,
  equalizerSelector: String,
) extends GrinderMLModel {
  private val imputer = Imputer(config.imputerConfig)
  private val tfidf = GroupedFeaturesTfidfTransformer(config.tfidfConfig)
  private val scaler = Scaler(config.scalerConfig)

  private val predictorWrapper = {
    val predictor = SGDClassifier(config.predictorConfig)
    predictor.init() // load pmml

    PredictorWrapper(predictor, config.predictorWrapperConfig)
  }

  private val slicer = Slicer(columns = Array(1)) // two classes, need second one

  private val equalizer = SBGroupedTransformer(
    config.equalizerConfig,
    group = equalizerSelector,
    transformerFactory = cfg => ScoreEqualizer(cfg.asInstanceOf[ScoreEqualizerConfig]),
  )

  // TODO: check stages parameters consistency before deciding that model is OK
  override def isOK: Boolean = true

  /** Beware: features vector inplace mutation!
    */
  override def apply(features: Array[Double]): Seq[Any] = {
    // features mutation here!
    val scores_raw: Array[Double] = applyPredictor(features)

    val scores: Array[Double] = scores_raw.clone()
    applyPostprocessor(scores)

    Seq(
      scores(0),
      scores_raw,
      scores,
      audienceName,
      "positive",
    )
  }

  /** Beware: features vector inplace mutation
    */
  private def applyPredictor(features: Array[Double]): Array[Double] = {
    // inplace update features vector
    imputer.transform(features)
    tfidf.transform(features)
    scaler.transform(features)

    // generate new vector with predictions
    val scores = predictorWrapper.predict(features)

    // extract raw score value
    slicer.transform(scores)
  }

  /** Beware: vec inplace mutation!
    */
  private def applyPostprocessor(vec: Array[Double]): Unit =
    equalizer.transform(vec)
}
