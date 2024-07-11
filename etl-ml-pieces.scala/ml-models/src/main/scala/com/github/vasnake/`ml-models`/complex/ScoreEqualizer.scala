/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`ml-core`.models.interface._
import com.github.vasnake.common.num.FastMath
import com.github.vasnake.core.num.SciPy.PPolyBernsteinCubic

case class ScoreEqualizerConfig(
  minval: Double,
  maxval: Double,
  noise: Double,
  eps: Double,
  coefficients: Array[Array[Double]],
  intervals: Array[Double],
  random_value: Option[Double] = None,
) extends PostprocessorConfig {
  override def toString: String =
    s"minval = ${minval}, maxval = ${maxval}, noise = $noise, eps = $eps, random_value = $random_value, " +
      s"intervals = ${intervals.length}, coefficients = ${coefficients.length},${coefficients.head.length}"
}

case class ScoreEqualizer(config: ScoreEqualizerConfig) extends InplaceTransformer {

  // TODO: split on two transformers: `UnitScaleStretcher` and `PCHIPInterpolator`
  //  so they could be used separately in `fit` and `transform` methods
  private val nIntervals = config.intervals.length
  require(nIntervals > 1, "intervals sequence must not be empty")
  require(config.coefficients.length == 4, "polynomial order must be = 3")
  require(
    config.coefficients.forall(_.length == nIntervals - 1),
    "must have coefficients for each interval",
  )

  require(
    // ascending
    config
      .intervals
      .indices
      .forall(idx => idx == 0 || config.intervals(idx) > config.intervals(idx - 1)) ||
    // descending
    config
      .intervals
      .indices
      .forall(idx => idx == 0 || config.intervals(idx) < config.intervals(idx - 1)),
    "wrong intervals endpoints",
  )

  private val random: () => Double = {
    val _random = config.random_value.getOrElse(Double.NaN)
    if (_random.isNaN) () => math.random // FastMath.random contains the same implementation
    else () => _random
  }

  private val maxmin = config.maxval - config.minval
  private val halfnoise = config.noise / 2.0
  private val stretchDelta = config.eps / (2.0 * (1.0 - 3.0 * config.eps))
  private val stretchLowX = stretchDelta
  private val stretchUpX = 1.0 - stretchDelta
  private val stretchUpLowX = stretchUpX - stretchLowX
  private val stretchLowUpX = 2.0 * config.eps * (stretchLowX + stretchUpX) - stretchLowX
  private val fourEps = 1.0 - 4.0 * config.eps

  def transform(vec: Array[Double]): Unit = {
    var idx: Int = 0
    vec.foreach { x =>
      if (!x.isNaN) vec(idx) = equalize(x)

      idx += 1
    }
  }

  @inline def transform(xval: Double): Double =
    if (xval.isNaN) xval
    else equalize(xval)

  @inline private def equalize(xval: Double): Double =
    // TODO: tests for performance impact for nested calls
    PPolyBernsteinCubic.interpolate(
      scaleAddNoiseAndStretch(xval),
      config.coefficients,
      config.intervals,
    )

  @inline def scaleAddNoiseAndStretch(xval: Double): Double =
    stretchEdgesUnitScale(
      if (config.noise > 0.0) addnoise((xval - config.minval) / maxmin, config.noise)
      else (xval - config.minval) / maxmin,
      config.eps,
    )

  @inline private def addnoise(xval: Double, noise: Double): Double =
    noise * FastMath.round(xval / noise) +
      (noise * random() - halfnoise)

  @inline private def stretchEdgesUnitScale(xval: Double, eps: Double): Double =
    if (xval < 0.0)
      eps * (1.0 - 2.0 * FastMath.atan(FastMath.sqrt(-xval)) / FastMath.PI)
    else if (xval >= 0.0 && xval <= stretchLowX)
      eps * (FastMath.sqrt(xval / stretchDelta) + 1.0)
    else if (xval > stretchLowX && xval < stretchUpX)
      (fourEps * xval + stretchLowUpX) / stretchUpLowX
    else if (xval >= stretchUpX && xval <= 1.0)
      1.0 - eps * (1.0 + FastMath.sqrt((1.0 - xval) / stretchDelta))
    else if (xval > 1.0)
      eps * (2.0 * FastMath.atan(FastMath.sqrt(xval - 1.0)) / FastMath.PI - 1.0) + 1.0
    else
      Double.NaN
}
