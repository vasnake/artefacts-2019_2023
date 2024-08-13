/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.shared

import scala.util.Try

import org.apache.spark.sql.types._

/** Parameters shared between QuantileThreshold estimator and transformer:
  * (inputCol, priorValues, labels, groupColumns, sampleSize, sampleRandomSeed, outputCol, rankCol)
  */
trait ScoreQuantileThresholdParams
    extends StratifiedModelParams
       with HasRankCol
       with HasPriorValues
       with HasLabels
       with SamplingModelParams
       with HasCacheSettings {
  override def validateAndTransformSchema(schema: StructType): StructType = {
    // check input columns existence
    validateInputColumns(schema)

    // check output column existence
    validateOutputColumn(Try(getOutputCol).toOption, schema, optional = false)
    validateOutputColumn(Try(getRankCol).toOption, schema, optional = true)

    val withLabel =
      if (isDefinedOutputCol) schema.add(StructField(getOutputCol, DataTypes.StringType))
      else schema

    if (isDefinedRankCol) withLabel.add(StructField(getRankCol, DataTypes.DoubleType))
    else withLabel
  }

  def getNumClasses: Int = getPriorValues.length

  def validateClassesPriorLabels(): Unit = {
    require(isDefinedPriorValues, "Prior values must be defined")
    require(
      isValidPriorValues(getPriorValues),
      "Prior values size must be > 1 and min value must be > 0",
    )
    require(
      !isDefinedLabels ||
      (getLabels.length == getNumClasses && getLabels.toSet.size == getNumClasses),
      "Labels must be empty or it's size must be = numClasses",
    )
  }

  setDefault(
    groupColumns -> Array.empty[String],
    sampleSize -> 100000,
    cacheSettings -> "cache",
  )
}
