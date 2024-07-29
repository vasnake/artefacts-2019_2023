/**
 * Created by vasnake@gmail.com on 2024-07-29
 */
package com.github.vasnake.spark.ml.estimator

//class NEPriorClassProbaEstimator {}

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.linalg.{Vectors, Vector => MLV}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.github.vasnake.spark.ml.shared._
import com.github.vasnake.spark.ml.model.{NEPriorClassProbaModel}
//import com.github.vasnake.`ml-models`.{complex => models}
import com.github.vasnake.`ml-core`.models.{NEPriorClassProba}
import com.github.vasnake.spark.dataset.transform.{StratifiedSampler}
import com.github.vasnake.spark.io.{Logging => CustomLogging}
//import com.github.vasnake.common.num.{FastMath => fm}

/**
  * Calculate aligned score probabilities according to given distribution in `prior` parameter.
  *
  * Supports stratification: different models could be learn/apply to different groups of rows.
  *
  * Fitting involves materialization of input data, you should cache input DataFrame before calling `fit`.
  */
class NEPriorClassProbaEstimator(override val uid: String) extends
  Estimator[NEPriorClassProbaModel] with
  NEPriorClassProbaParams with
  ParamsServices with
  CustomLogging with
  DefaultParamsWritable
{
  def this() = this(Identifiable.randomUID("ne_prior_class_proba"))

  override def copy(extra: ParamMap): Estimator[NEPriorClassProbaModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  def fit(dataset: Dataset[_]): NEPriorClassProbaModel = {
    // stratification on groups, sampling, fit model for each group ...
    import NEPriorClassProbaModel.json
    logInfo("Fit models ...")
    logDebug(s"with params:\n${json(params.map(explain))}")

    validatePriorValues()
    require(getSampleSize > 0, s"Sample size must be > 0, got `${getSampleSize}`")
    require(getNumClasses > 1, s"Number of classes must be > 1, got `${getNumClasses}`")
    validateInputColumns(dataset.schema)

    val configs: List[(String, MLV)] = {

      // stratification and sampling
      val group_score: DataFrame = {
        val cfg = StratifiedSampler.SelectGroupScoreSampleConfig(
          sampleRandomSeed = if (isDefinedSampleRandomSeed) getSampleRandomSeed else Double.NaN,
          sampleSize = getSampleSize,
          groupColumnsList = getGroupColumns.toList,
          inputColName = getInputCol,
          inputCastType = "array<double>",
          validInputExpr = "score is not null and " +
            s"size(score) = ${getNumClasses} and " +
            s"not exists(score, x -> isnull(x) or isnan(x) or x < 0) and " +
            s"exists(score, x -> x > 0)",
          cacheFunction = cacheFunction
        )

        // Dataset[(group, score)], cached
        StratifiedSampler.getGroupScoreSample(dataset, cfg)
      }

      // distributed fit, (group, message, model)
      val group_model: Array[(String, String, Option[Array[Double]])] = NEPriorClassProbaEstimator
        .distributedFit(group_score, getNumClasses, getPriorValues)
        .collect()

      group_score.unpersist()

      // report errors and project models
      group_model.toList.flatMap {
        case (group, msg, Some(arr)) =>
          if (msg.nonEmpty) logWarning(s"group `$group`, fit problems: $msg")
          Some((group, Vectors.dense(arr)))
        case (group, msg, None) =>
          logWarning(s"group `${group}`, fit has failed: $msg")
          None
      }
    }

    logInfo(s"Fit completed, models count: ${configs.length}")
    logDebug(s"Models data:\n${json(configs)}")
    if (configs.isEmpty) logWarning("Fit should produce at least one model, 0 models fitted")

    copyValues(new NEPriorClassProbaModel(uid, configs)).setParent(this)
  }

}

object NEPriorClassProbaEstimator {

  // invalid data filtered out already
  def distributedFit(group_score: DataFrame, numClasses: Int, priorValues: Array[Double]): Dataset[(String, String, Option[Array[Double]])] = {
    import group_score.sparkSession.implicits._
    val ds = group_score.as[(String, Array[Double])]

    val fitOnGroup: (String, Iterator[(String, Array[Double])]) => (String, String, Option[Array[Double]]) =
      (group, rows) => {
        val res = fit(rows.map(_._2), numClasses, priorValues)
        (group, res._1, res._2)
      }

    ds.groupByKey(_._1).mapGroups(fitOnGroup)
  }

  def fit(probs: Iterator[Array[Double]], numClasses: Int, priorValues: Array[Double]): (String, Option[Array[Double]]) =
    NEPriorClassProba.fit(probs, numClasses, priorValues)

}
