/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.common.num.NumPy.percentile
import com.github.vasnake.core.num.NumPy._

/** Implementation of ScoreQuantileThresholdTransformer from closed python module.
  *
  * Calculates score label (bin index) according to distribution given in `prior` parameter.
  */
case class ScoreQuantileThreshold(config: ScoreQuantileThresholdConfig) {

  /** Return score 'label' i.e. class index
    *
    * @param score score value
    * @return index: class index starting from 1; if score is nan returns 0
    */
  @inline def transform(score: Double): Int =
    if (score.isNaN) 0
    else digitize(score, config.thresholds, right = false)

  /** Return score rank in its class
    *
    * @param score      score value
    * @param classIndex score 'label': class index starting from 1
    * @return score rank [0..1] where score on class edges produces 0 and score in the middle of class produces 1.
    *         If score.isNaN or classIndex = 0: return nan
    */
  @inline def rank(score: Double, classIndex: Int): Double =
    if (score.isNaN || classIndex <= 0) Double.NaN
    else {
      val (left, middle, right) = config.intervals(classIndex)
      if (score <= middle)
        if (left.isNaN) 1.0
        else (score - left) / (middle - left)
      else if (right.isNaN) 1.0
      else (right - score) / (right - middle)
    }
}

object ScoreQuantileThreshold {
  def fit(xs: Array[Double], prior: Array[Double]): ScoreQuantileThresholdConfig = {
    // TODO: validate parameters

    val quantiles: Array[Double] = percentile(xs, cumulativePercents(prior))

    val thresholds: Array[Double] = slice(quantiles, 0, prior.length, 1)
    thresholds(0) = Double.MinValue

    val intervals: Map[Int, (Double, Double, Double)] = prior
      .indices
      .map {
        case a if a == 0 =>
          1 -> (Double.NaN, quantiles(0), quantiles(1))
        case b if b == prior.length - 1 =>
          (b + 1) -> (quantiles(b), quantiles(b + 1), Double.NaN)
        case c =>
          (c + 1) -> (quantiles(c), (quantiles(c) + quantiles(c + 1)) / 2d, quantiles(c + 1))
      }
      .toMap

    ScoreQuantileThresholdConfig(thresholds, intervals)
  }

  def cumulativePercents(prior: Array[Double]): Array[Double] = {
    val sum: Double = prior.sum
    val norm: Array[Double] = 0d +: prior.map(p => 100 * p / sum)

    val res: Array[Double] = cumulativeSum(norm)
    res(res.length - 1) = 100

    res
  }
}

/** Data learned on `fit` stage of ScoreQuantileThreshold model and used on `transform` stage.
  *
  * @param thresholds classes bins
  * @param intervals map class => (left, middle, right), class interval points
  */
case class ScoreQuantileThresholdConfig(
  thresholds: Array[Double],
  intervals: Map[Int, (Double, Double, Double)],
)
