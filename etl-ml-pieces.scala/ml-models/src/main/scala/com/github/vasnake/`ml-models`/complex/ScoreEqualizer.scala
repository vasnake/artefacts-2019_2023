/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`ml-core`.models.interface._
import com.github.vasnake.core.num.SciPy.PPolyBernsteinCubic
import com.github.vasnake.common.num.{FastMath => fm}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

// TODO: move to ml-core
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
    noise * fm.round(xval / noise) +
      (noise * random() - halfnoise)

  @inline private def stretchEdgesUnitScale(xval: Double, eps: Double): Double =
    if (xval < 0.0)
      eps * (1.0 - 2.0 * fm.atan(fm.sqrt(-xval)) / fm.PI)
    else if (xval >= 0.0 && xval <= stretchLowX)
      eps * (fm.sqrt(xval / stretchDelta) + 1.0)
    else if (xval > stretchLowX && xval < stretchUpX)
      (fourEps * xval + stretchLowUpX) / stretchUpLowX
    else if (xval >= stretchUpX && xval <= 1.0)
      1.0 - eps * (1.0 + fm.sqrt((1.0 - xval) / stretchDelta))
    else if (xval > 1.0)
      eps * (2.0 * fm.atan(fm.sqrt(xval - 1.0)) / fm.PI - 1.0) + 1.0
    else
      Double.NaN
}

object ScoreEqualizer { // TODO: with Estimator trait

  // TODO: use case class(msg, optional model)
  /**
    * Return error message (or empty string) and fitted model parameters
    */
  def fit(
                         scoreRaw: Iterator[Double],
                         minInputSize: Int,
                         noiseValue: Double,
                         epsValue: Double,
                         randomValue: Double,
                         numBins: Int
                       ): (String, Option[ScoreEqualizerConfig]) = Try {

    // compute min,max: fail if no data; default minmax = 0..1 if only one unique value
    var minval: Double = Double.MaxValue
    var maxval: Double = Double.MinValue

    val arr: Array[Double] = {
      val buf: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.empty[Double] // n.b. null converted to 0

      scoreRaw.foreach(v => {
        minval = fm.min(minval, v)
        maxval = fm.max(maxval, v)
        buf.append(v)
      })

      buf.toArray
    }

    require(arr.length >= minInputSize, s"Collecting data statistics has failed. No values found to estimate scores CDF, input.size = ${arr.length}")

    val msg = if (minval >= maxval) {
      minval = 0.0
      maxval = 1.0

      "'X' contains single unique value, equalization cannot be done properly"
    } else ""

    val config = ScoreEqualizerConfig(
      noise = noiseValue,
      eps = epsValue,
      random_value = if (randomValue.isNaN) None else Some(randomValue),
      minval = minval,
      maxval = maxval,
      // TODO: we don't need cdf right here, ScoreEqualizer should be split on two transformers: normalizer and equalizer
      coefficients = (0 to 3).map(_ => Array(0.0)).toArray,
      intervals = Array(0.0, 1.0)
    )

    // scale, add noise, stretch
    val stretchedScore: Array[Double] = {
      val eq = ScoreEqualizer(config)

      arr.map(x => eq.scaleAddNoiseAndStretch(x))
    }

    // PCHIP spline intervals and coefficients // (Piecewise Cubic Hermite Interpolating Polynomial; Cumulative Distribution Function)
    val cdf: CDFCoefficients = fitCDF(stretchedScore, numBins)

    (msg, config.copy(coefficients = cdf.coefficients, intervals = cdf.intervals))

  } match {
    case Success((msg, cfg)) => (msg, Some(cfg))
    case Failure(err) => (err.getMessage, None)
  }

  /**
    * Cumulative Distribution Function
    * for
    * Piecewise Cubic Hermite Interpolating Polynomial
    * with uniform distribution in [0, 1] as target
    */
  def fitCDF(xs: Array[Double], numBins: Int): CDFCoefficients = {
    import com.github.vasnake.core.num.NumPy.{slice, histogram, cumulativeSum}
    import com.github.vasnake.core.num.SciPy.PCHIP

    // sort, select unique; find (step(numbins), max, delta);
    // select unique bins edges; compute (histogram, edges); compute y(histogram);
    // call pchip-interpolator(edges, y)

    val sorted = xs.distinct.sorted
    val step = fm.ceil(sorted.length.toDouble / numBins.toDouble).toInt
    val maxScore = sorted.last
    val delta = fm.min((1.0 - maxScore) / 2.0, 1e-12)

    // edges
    val bins =
      (Seq(0.0) ++
        slice(sorted, 0, sorted.length - step + 1, step) ++
        Seq(maxScore + delta, 1.0)).toArray.distinct

    val hist: Array[Int] = histogram(xs, bins)
    // TODO: collect stat and metrics
    if (!hist.indices.forall(idx => idx == 0 || idx == hist.length - 1 || hist(idx) != 0)) println("There is a constant CDF interval")

    val ys: Array[Double] = {
      val sum = hist.sum.toDouble

      0.0 +: cumulativeSum(hist).map(_.toDouble / sum)
    }

    CDFCoefficients(
      coefficients = PCHIP.coefficients(bins, ys),
      intervals = bins
    )
  }

  case class CDFCoefficients(intervals: Array[Double], coefficients: Array[Array[Double]])
}

