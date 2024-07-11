/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

object interface {
  trait GroupedFeaturesTransformer { // TODO: rename to InplaceTransformer
    def transform(input: Array[Double]): Unit
  }
}
