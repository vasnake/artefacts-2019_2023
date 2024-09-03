/** Created by vasnake@gmail.com on 2024-08-09
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`etl-core`._
import com.github.vasnake.`ml-core`.models._
import com.github.vasnake.test.{ Conversions => CoreConversions }
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class LalBinarizedMultinomialNbTest extends AnyFlatSpec with should.Matchers {
  import CoreConversions.implicits._

  it should "create new model from config" in {
    val config = {
      val imputerConfig = Array.empty[Float]
      val binarizerConfig = BinarizerConfig(threshold = 0f)

      val predictorWrapperConfig = PredictorWrapperConfig(
        minFeaturesPerSample = 1,
        maxFeaturesPerSample = 10000,
        predictLength = 2
      )

      val predictorConfig = MultinomialNBConfig(
        featuresLength = 1,
        predictLength = 2,
        classLogPrior = Array(0f, 0f),
        featureLogProb = Array(Array(0d), Array(0d))
      )

      val equalizerConfig = SBGroupedTransformerConfig(groups = Map.empty)

      LalBinarizedMultinomialNbModelConfig(
        imputerConfig,
        binarizerConfig,
        predictorWrapperConfig,
        predictorConfig,
        equalizerConfig
      )
    }

    val groupedFeatures = GroupedFeatures(groups = Seq.empty[FeaturesGroup])

    val model = LalBinarizedMultinomialNbModel(
      config,
      groupedFeatures,
      audienceName = "bar",
      equalizerSelector = ""
    )
    assert(model.isOK === true)
  }
  // TODO: add tests
  //  fail on corrupted config
  //  read config from file
  //  fail on empty input
  //  produce simple prediction
  //  produce reference prediction

}
