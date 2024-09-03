/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.`ml-core`.models.interface.GroupedFeaturesTransformer

case class Scaler(config: ScalerConfig) extends GroupedFeaturesTransformer {
  if (config.withMean) require(config.means.length > 0, "array of mean values must not be empty")
  if (config.withStd) require(config.scales.length > 0, "array of scale values must not be empty")
  if (config.withStd && config.withMean)
    require(
      config.scales.length == config.means.length,
      "mean and scale arrays length must be equal"
    )

  private val nFeatures =
    if (config.withStd) config.scales.length
    else if (config.withMean) config.means.length
    else 0

  override def transform(input: Array[Double]): Unit = {
    require(
      (nFeatures > 0 && input.length == nFeatures) ||
      (nFeatures == 0 && input.length > 0),
      "input vector must not be empty and size must correspond to coeff. vector size"
    )

    if (nFeatures > 0) {
      var idx: Int = 0
      input.foreach { x =>
        if (config.withMean) input(idx) = x - config.means(idx)
        if (config.withStd) input(idx) = input(idx) / config.scales(idx)

        idx += 1
      }
    }
  }
}

case class ScalerConfig(
  withMean: Boolean = false,
  withStd: Boolean = false,
  means: Array[Double] = Array.empty,
  scales: Array[Double] = Array.empty
)
