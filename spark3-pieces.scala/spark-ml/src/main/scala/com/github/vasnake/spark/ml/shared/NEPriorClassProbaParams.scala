/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.shared

import scala.util.Try

import org.apache.spark.sql.types._

/** Parameters shared between NEPriorClassProba estimator and model (transformer):
  * (inputCol, numClasses, priorValues, groupColumns, sampleSize, sampleRandomSeed, outputCol)
  */
trait NEPriorClassProbaParams
    extends StratifiedModelParams
       with HasPriorValues
       with SamplingModelParams
       with HasCacheSettings {
  override def validateAndTransformSchema(schema: StructType): StructType = {
    // check input columns existence
    validateInputColumns(schema)
    // check output column existence
    validateOutputColumn(Try(getOutputCol).toOption, schema, optional = false)
    // transform schema
    schema.add(StructField(getOutputCol, DataTypes.createArrayType(DataTypes.DoubleType)))
  }

  def getNumClasses: Int = getPriorValues.length

  def validatePriorValues(): Unit = {
    require(isDefinedPriorValues, "Prior values must be defined")
    require(
      isValidPriorValues(getPriorValues),
      "Prior values size must be > 1 and min value must be > 0"
    )
  }

  setDefault(
    sampleSize -> 100000,
    groupColumns -> Array.empty[String],
    cacheSettings -> "cache"
  )
}
