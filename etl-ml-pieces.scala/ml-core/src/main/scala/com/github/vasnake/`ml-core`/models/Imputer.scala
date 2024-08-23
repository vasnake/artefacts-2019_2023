/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.`ml-core`.models.interface.GroupedFeaturesTransformer

case class Imputer(imputedValues: Array[Double]) extends GroupedFeaturesTransformer {
  def transform(input: Array[Double]): Unit = {
    require(
      input.length == imputedValues.length,
      "input vector size must be equal imputed values vector size"
    )

    var idx: Int = 0
    input.foreach { x =>
      if (x.isNaN) input(idx) = imputedValues(idx)

      idx += 1
    }
  }
}
