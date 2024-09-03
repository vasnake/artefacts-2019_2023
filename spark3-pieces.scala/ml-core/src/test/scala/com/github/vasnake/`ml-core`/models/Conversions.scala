/** Created by vasnake@gmail.com on 2024-08-08
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.`ml-core`.models.interface.GroupedFeaturesTransformer

object Conversions {
  class ExtendedTransformer(tr: GroupedFeaturesTransformer) {
    def _transform(vec: Array[Double]): Array[Double] = {
      val output = vec.clone()
      tr.transform(output)
      output
    }
  }

  object implicits {
    import scala.language.implicitConversions

    implicit def transformer2ExtendedTransformer(tr: GroupedFeaturesTransformer)
      : ExtendedTransformer = new ExtendedTransformer(tr)
  }
}
