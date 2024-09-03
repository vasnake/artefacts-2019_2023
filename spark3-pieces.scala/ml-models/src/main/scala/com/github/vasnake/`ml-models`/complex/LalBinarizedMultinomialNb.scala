/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`etl-core`.GroupedFeatures
import com.github.vasnake.`ml-core`.models._

case class LalBinarizedMultinomialNbModelConfig(
  imputerConfig: Array[Double],
  binarizerConfig: BinarizerConfig,
  predictorWrapperConfig: PredictorWrapperConfig,
  predictorConfig: MultinomialNBConfig,
  equalizerConfig: SBGroupedTransformerConfig
)

case class LalBinarizedMultinomialNbModel // TODO: rename to LalBinarizedMultinomialNb
(
  config: LalBinarizedMultinomialNbModelConfig,
  groupedFeatures: GroupedFeatures,
  audienceName: String,
  equalizerSelector: String
) extends ComplexMLModel {
  private val imputer = Imputer(config.imputerConfig)
  private val binarizer = Binarizer(config.binarizerConfig)

  private val predictorWrapper = PredictorWrapper(
    MultinomialNB(config.predictorConfig),
    config.predictorWrapperConfig
  )

  private val slicer = Slicer(columns = Array(1)) // two target classes, need second one

  private val equalizer = SBGroupedTransformer(
    config.equalizerConfig,
    group = equalizerSelector,
    transformerFactory = cfg => ScoreEqualizer(cfg.asInstanceOf[ScoreEqualizerConfig])
  )

  // TODO: check stages parameters consistency before deciding that model is OK
  override def isOK: Boolean = true

  /** Beware: features vector inplace mutation!
    */
  override def apply(features: Array[Double]): Seq[Any] = {
    // features mutation here!
    val scores_raw: Array[Double] = applyPredictor(features)

    val scores: Array[Double] = scores_raw.clone()
    // scores mutation
    applyPostprocessor(scores)

    Seq(
      scores(0), // equalized score value
      scores_raw, // raw scores array
      scores, // equalized scores array
      audienceName, // project id (target)
      "positive" // category
    )
  }

  /** Beware: features vector inplace mutation
    */
  private def applyPredictor(features: Array[Double]): Array[Double] = {
    // inplace update features vector
    imputer.transform(features)
    binarizer.transform(features)

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
