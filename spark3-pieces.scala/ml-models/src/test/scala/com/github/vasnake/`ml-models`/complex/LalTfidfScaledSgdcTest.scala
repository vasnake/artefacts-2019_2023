/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`etl-core`._
import com.github.vasnake.`ml-core`.models._
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.test.{ Conversions => CoreConversions }
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class LalTfidfScaledSgdcTest extends AnyFlatSpec with should.Matchers {
  import CoreConversions.implicits._

  it should "create new model from config" in {
    val config = {
      val imputerConfig = Array.empty[Float]

      val tfidfConfig = GroupedFeaturesTfidfTransformerConfig(
        transformer_params = Map(
          "norm" -> "l1",
          "smooth_idf" -> "true",
          "sublinear_tf" -> "false",
          "use_idf" -> "true"
        )
      )

      val scalerConfig = ScalerConfig()
      val predictorWrapperConfig = PredictorWrapperConfig(
        minFeaturesPerSample = 1,
        maxFeaturesPerSample = 10000,
        predictLength = 2
      )

      val predictorConfig = PMMLEstimatorConfig(
        featuresLength = 4,
        predictLength = 3,
        fileName = FileToolbox.getResourcePath(this, "/sgd_classifier.pmml")
      )

      val equalizerConfig = SBGroupedTransformerConfig(groups = Map.empty)

      LalTfidfScaledSgdcModelConfig(
        imputerConfig,
        tfidfConfig,
        scalerConfig,
        predictorWrapperConfig,
        predictorConfig,
        equalizerConfig
      )
    }

    val groupedFeatures = GroupedFeatures(groups = Seq.empty[FeaturesGroup])

    val model =
      LalTfidfScaledSgdcModel(config, groupedFeatures, audienceName = "bar", equalizerSelector = "")
    assert(model.isOK === true)
  }
  // TODO: add tests
  //  fail on corrupted config
  //  read config from file
  //  fail on empty input
  //  produce simple prediction
  //  produce reference prediction

}
