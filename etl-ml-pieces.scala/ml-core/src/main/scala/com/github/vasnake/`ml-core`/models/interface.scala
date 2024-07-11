/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

object interface {
  trait GroupedFeaturesTransformer { // TODO: rename to InplaceTransformer
    def transform(input: Array[Double]): Unit
  }

  trait InplaceTransformer {
    def transform(vec: Array[Double]): Unit
  }

  trait Estimator { // TODO: rename to Transformer.transform
    def predict(features: Array[Double]): Array[Double]
  }

  trait PMMLEstimator extends Estimator { // TODO: move to PMML module
    def init(): Unit
  }

  trait PostprocessorConfig
}
