/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`ml-core`.models.interface.Estimator
import com.github.vasnake.common.num.FastMath
// TODO: move to ml-core
case class MultinomialNB(config: MultinomialNBConfig) extends Estimator {
  // `predict_proba` from
  // https://github.com/scikit-learn/scikit-learn/blob/3743a55aed5c9ee18c8b94b95dfc4c41d0ae99f5/sklearn/naive_bayes.py#L102
  require(
    config.featuresLength > 0 && config.predictLength > 0,
    "features length and number of classes must be greater than zero"
  )
  require(
    config.featureLogProb.forall(_.length == config.featuresLength),
    "features length must be consistent in config parameters"
  )
  require(
    config.predictLength == config
      .classLogPrior
      .length && config.predictLength == config.featureLogProb.length,
    "number of classes must be consistent in config parameters"
  )

  override def predict(features: Array[Double]): Array[Double] = {
    require(
      features.length == config.featuresLength,
      "features vector must have size declared in config"
    )
    // version with C-style loops 3-times faster than scala collections operators
    var jllMax: Double = Double.MinValue
    var jllSumExp: Double = 0
    var classIdx: Int = 0

    // _joint_log_likelihood
    val classesProb: Array[Double] = {
      val res = new Array[Double](config.predictLength)
      var featureIdx: Int = 0

      classIdx = 0
      config.featureLogProb.foreach { flp =>
        featureIdx = 0
        features.foreach { x =>
          res(classIdx) = res(classIdx) + flp(featureIdx) * x

          featureIdx += 1
        }

        res(classIdx) = res(classIdx) + config.classLogPrior(classIdx)
        if (res(classIdx) > jllMax) jllMax = res(classIdx)

        classIdx += 1
      }

      res
    }

    val logProbX = {
      // log_sum_exp
      classIdx = 0
      classesProb.foreach { x =>
        jllSumExp += FastMath.exp(x - jllMax)

        classIdx += 0
      }

      FastMath.log(jllSumExp) + jllMax
    }

    // result
    classIdx = 0
    classesProb.foreach { x =>
      classesProb(classIdx) = FastMath.exp(x - logProbX)

      classIdx += 1
    }

    classesProb
  }
}

case class MultinomialNBConfig(
  featuresLength: Int,
  predictLength: Int,
  classLogPrior: Array[Double],
  featureLogProb: Array[Array[Double]]
)
