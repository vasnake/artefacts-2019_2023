/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.shared

import org.apache.spark.ml.param._

/** Parameters shared between estimator and model (transformer) of ScoreEqualizer:
  * (inputCol, outputCol, groupColumns, sampleSize, sampleRandomSeed, numBins, noiseValue, epsValue, randomValue)
  */
trait ScoreEqualizerParams
    extends StratifiedModelParams
       with HasNumClasses
       with SamplingModelParams
       with HasRandomValue
       with HasCacheSettings {
  final val numBins: IntParam = new IntParam(
    this,
    "numBins",
    "Fit: number of bins for spline interpolator",
    isValid = ParamValidators.gt(0),
  )
  def getNumBins: Int = $(numBins)
  def setNumBins(value: Int): this.type = {
    require(value > 0, "Num bins must be a positive number")
    set(numBins, value)
  }

  final val noiseValue: DoubleParam = new DoubleParam(this, "noiseValue", "Random noise addition")
  def getNoiseValue: Double = $(noiseValue)
  def setNoiseValue(value: Double): this.type = set(noiseValue, value)

  final val epsValue: DoubleParam =
    new DoubleParam(this, "epsValue", "Epsilon parameter for unit stretching")
  def getEpsValue: Double = $(epsValue)
  def setEpsValue(value: Double): this.type = set(epsValue, value)

  final val minInputSize: IntParam = new IntParam(
    this,
    "minInputSize",
    "Fit: minimum size of train vector",
    isValid = ParamValidators.gt(0),
  )
  def getMinInputSize: Int = $(minInputSize)
  def setMinInputSize(value: Int): this.type = {
    require(value > 0, "Min input size must be a positive number")
    set(minInputSize, value)
  }

  setDefault(
    groupColumns -> Array.empty[String],
    sampleSize -> 100000,
    numBins -> 10000,
    noiseValue -> 1e-4,
    epsValue -> 1e-3,
    minInputSize -> 1,
    cacheSettings -> "cache",
  )
}
