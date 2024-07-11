/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.`ml-core`.models.interface.GroupedFeaturesTransformer

case class Binarizer(config: BinarizerConfig) extends GroupedFeaturesTransformer {
  private val threshold: Double = config.threshold

  override def transform(input: Array[Double]): Unit = {
    require(input.length > 0, "vector must not be empty")

    var idx: Int = 0
    input.foreach { x =>
      if (x > threshold) input(idx) = 1.0
      else input(idx) = 0.0

      idx += 1
    }
  }
}

case class BinarizerConfig(threshold: Double = 0)
