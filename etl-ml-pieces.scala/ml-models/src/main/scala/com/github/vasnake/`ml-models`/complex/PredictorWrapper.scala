/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`ml-core`.models.interface.Estimator

case class PredictorWrapper(predictor: Estimator, config: PredictorWrapperConfig)
    extends Estimator {
  override def predict(features: Array[Double]): Array[Double] = {
    require(features.length > 0, "wrong input vector size")

    var featuresCount: Int = 0 // faster than: features.count(x => x != 0.0)
    features.foreach(x => if (x != 0.0) featuresCount += 1)

    if (
        featuresCount >= config.minFeaturesPerSample && featuresCount <= config.maxFeaturesPerSample
    )
      predictor.predict(features)
    else {
      // faster than: (0 until config.predictLength).map(_ => Double.NaN).toArray
      val res = new Array[Double](config.predictLength)
      java.util.Arrays.fill(res, Double.NaN)

      res
    }
  }
}

case class PredictorWrapperConfig(
  minFeaturesPerSample: Int,
  maxFeaturesPerSample: Int,
  predictLength: Int,
)
