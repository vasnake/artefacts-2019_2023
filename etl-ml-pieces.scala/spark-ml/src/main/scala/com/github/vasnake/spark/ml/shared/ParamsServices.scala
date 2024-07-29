/**
 * Created by vasnake@gmail.com on 2024-07-29
 */
package com.github.vasnake.spark.ml.shared

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, IntParam, ParamValidators, StringArrayParam}
import org.apache.spark.sql.DataFrame

import scala.util.Try

trait ParamsServices {
  this: Params =>

  /**
    * Replacement for params.explainParam
    */
  def explain(param: Param[_]): ParamExplained =
    ParamExplained(
      name = param.name,
      value = get(param).map(p => param2String(p)).getOrElse("None"),
      doc = param.doc,
      default = getDefault(param).map(p => param2String(p)).getOrElse("None")
    )

  def param2String[T](p: T): String = p match {
    case a: Array[_] => a.mkString("Array(", ", ", ")")
    case b => b.toString
  }

}

case class ParamExplained(name: String, value: String, doc: String, default: String)

trait HasOutputCol extends Params {

  /**
    * Param for output column name.
    * @group param
    */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")
  final def getOutputCol: String = $(outputCol)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def isDefinedOutputCol: Boolean = Try(getOutputCol.nonEmpty).getOrElse(false)
}

trait HasNumClasses extends Params {

  /**
    * Param for number of classes.
    * @group param
    */
  final val numClasses: IntParam = new IntParam(this, "numClasses", "number of classes", isValid = ParamValidators.gt(1))

  /** @group getParam */
  def getNumClasses: Int = $(numClasses)

  def setNumClasses(value: Int): this.type = {
    require(value > 1, "Number of classes must be not less than 2")
    set(numClasses, value)
  }

  def isDefinedNumClasses: Boolean = Try($(numClasses) > 1).getOrElse(false)
}

trait HasRandomValue extends Params {

  val isValidRandomValue: Double => Boolean =
    x => { 0.0 <= x && x <= 1.0 }

  final val randomValue: DoubleParam = new DoubleParam(this, "randomValue", "optional `math.random` replacement", isValid = isValidRandomValue)

  def getRandomValue: Double = $(randomValue)

  def setRandomValue(value: Double): this.type = {
    require(isValidRandomValue(value), "Random value must be in range [0, 1]")
    set(randomValue, value)
  }

  def isDefinedRandomValue: Boolean = Try(!getRandomValue.isNaN).getOrElse(false)
}

trait HasCacheSettings extends Params {
  // available cache options: `cache`

  def cacheFunctions: PartialFunction[String, DataFrame => DataFrame] = {
    case a: String if a.trim.toUpperCase == "CACHE" => df => df.cache()
    // TODO: persist:level, checkpoint:path, parquet:path
  }

  // empty string is valid settings, meaning "no cache"
  val isValidCacheSettings: String => Boolean = cs => cs.isEmpty || cacheFunctions.isDefinedAt(cs)

  final val cacheSettings: Param[String] = new Param[String](this, "cacheSettings", "DataFrame cache settings")

  def getCacheSettings: String = $(cacheSettings)

  def setCacheSettings(value: String): this.type = {
    require(isValidCacheSettings(value), s"Unknown cache settings: `${value}`")
    set(cacheSettings, value)
  }

  def cacheFunction: Option[DataFrame => DataFrame] = cacheFunctions.lift(getCacheSettings)

  setDefault(cacheSettings -> "")
}

trait HasLabels extends Params {

  val isValidLabels: Array[String] => Boolean =
    labels => { labels.isEmpty || labels.toSet.size == labels.length }

  /**
   * Param for set of unique labels.
   * @group param
   */
  final val labels: StringArrayParam = new StringArrayParam(this, "labels", "label values", isValid = isValidLabels)

  /** @group getParam */
  final def getLabels: Array[String] = $(labels)

  def setLabels(value: Iterable[String]): this.type = {
    require(isValidLabels(value.toArray), s"Labels list must be empty or contain unique values, got `${value.mkString(", ")}`")
    set(labels, value.toArray)
  }

  def isDefinedLabels: Boolean = Try(getLabels.nonEmpty).getOrElse(false)
}

trait HasRankCol extends Params {
  final val rankCol: Param[String] = new Param[String](this, "rankCol", "Column name with rank values")
  def getRankCol: String = $(rankCol)
  def setRankCol(value: String): this.type = set(rankCol, value)
  def isDefinedRankCol: Boolean = Try(getRankCol.nonEmpty).getOrElse(false)
}

trait HasPriorValues extends Params {

  val isValidPriorValues: Array[Double] => Boolean =
    xs => { xs.length > 1 && xs.min > 0 }

  final val priorValues: DoubleArrayParam = new DoubleArrayParam(this, "priorValues", "Prior classes distribution ratio", isValid = isValidPriorValues)

  def getPriorValues: Array[Double] = $(priorValues)

  def setPriorValues(value: Iterable[Double]): this.type = {
    require(isValidPriorValues(value.toArray), "Prior size must be > 1 and min value must be > 0")
    set(priorValues, value.toArray)
  }

  def isDefinedPriorValues: Boolean = Try(getPriorValues.length > 0).getOrElse(false)
}

trait HasWeightValue extends Params {

  final val weightValue: DoubleParam = new DoubleParam(this, "weightValue", "weight coefficient", isValid = ParamValidators.gt(0.0))

  def getWeightValue: Double = $(weightValue)

  def setWeightValue(value: Double): this.type = {
    require(value > 0.0, "Oldest weight must be a positive number (0, 1]")
    set(weightValue, value)
  }

  setDefault(weightValue -> 1.0)
}
