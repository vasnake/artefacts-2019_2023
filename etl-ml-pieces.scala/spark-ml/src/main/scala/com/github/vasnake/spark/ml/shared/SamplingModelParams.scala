/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.shared

import scala.util.Try

import org.apache.spark.ml.param._

/** Add `sampleSize`, `sampleRandomSeed` parameters: max number of rows in single sample and seed for sql.functions.random function.
  */
trait SamplingModelParams extends Params {
  final val sampleSize: IntParam = new IntParam(
    this,
    "sampleSize",
    "Fit: max number of sample rows for each group",
    isValid = ParamValidators.gt(0)
  )
  def getSampleSize: Int = $(sampleSize)
  def setSampleSize(value: Int): this.type = {
    require(value > 0, "Sample size must be a positive number")
    set(sampleSize, value)
  }

  final val sampleRandomSeed: DoubleParam =
    new DoubleParam(this, "sampleRandomSeed", "seed value for sampling random function")
  def getSampleRandomSeed: Double = $(sampleRandomSeed)
  def setSampleRandomSeed(value: Double): this.type = set(sampleRandomSeed, value)
  def isDefinedSampleRandomSeed: Boolean = Try(!getSampleRandomSeed.isNaN).getOrElse(false)

  setDefault(
    sampleSize -> 100000
  )
}
