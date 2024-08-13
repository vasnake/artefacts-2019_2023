/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.spark.ml.shared

import scala.util.Try

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.sql.types._

/** Parameters: (inputCol, outputCol, groupColumns).
  */
trait StratifiedModelParams extends Params with HasInputCol with HasOutputCol {
  val isValidNamesSet: Array[String] => Boolean = names =>
    names.isEmpty || names.toSet.size == names.length
  final val groupColumns: StringArrayParam = new StringArrayParam(
    this,
    "groupColumns",
    "stratification columns names",
    isValid = isValidNamesSet,
  )
  def getGroupColumns: Array[String] = $(groupColumns)

  def setGroupColumns(value: Iterable[String]): this.type = {
    require(
      isValidNamesSet(value.toArray),
      s"Group columns list must be empty or contain unique values, got `${value.mkString(", ")}`",
    )
    set(groupColumns, value.toArray)
  }

  def setInputCol(value: String): this.type = set(inputCol, value)
  def isDefinedInputCol: Boolean = Try(getInputCol.nonEmpty).getOrElse(false)

  def validateAndTransformSchema(schema: StructType): StructType = {
    // check input columns existence
    validateInputColumns(schema)
    // check output column existence
    validateOutputColumn(Try(getOutputCol).toOption, schema, optional = false)
    // transform schema
    schema.add(StructField($(outputCol), DataTypes.DoubleType))
  }

  def validateInputColumns(schema: StructType): Unit = {
    require(isDefinedInputCol, "Input column must be defined")

    (Seq(getInputCol) ++ getGroupColumns)
      .foreach(colname => schema.fieldIndex(colname))
  }

  def validateOutputColumn(
    colname: Option[String],
    schema: StructType,
    optional: Boolean,
  ): Unit = {
    val defined = colname.getOrElse("").nonEmpty
    require(optional || defined, "Output column name can't be empty")
    require(
      !defined || Try(schema.fieldIndex(colname.get)).isFailure,
      s"Output column `${colname.get}` already exists but mustn't",
    )
  }

  setDefault(
    groupColumns -> Array.empty[String]
  )
}
