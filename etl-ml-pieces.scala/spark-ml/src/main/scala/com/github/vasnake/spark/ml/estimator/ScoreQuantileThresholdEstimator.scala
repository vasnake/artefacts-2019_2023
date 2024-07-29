/**
 * Created by vasnake@gmail.com on 2024-07-29
 */
package com.github.vasnake.spark.ml.estimator

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

import com.github.vasnake.`ml-core`.models
import com.github.vasnake.spark.dataset.transform.StratifiedSampler
import com.github.vasnake.spark.io.{Logging => CustomLogging}
import com.github.vasnake.spark.ml.shared._
import com.github.vasnake.spark.ml.model.ScoreQuantileThresholdModel

/**
  * Calculates score label (class index) according to distribution given in `prior` parameter.
  *
  * Supports stratification: different models could be learn/apply to different groups of rows.
  *
  * Because fitting involves materialization of input data, you should persist input DataFrame before calling `fit`.
  */
class ScoreQuantileThresholdEstimator(override val uid: String) extends
  Estimator[ScoreQuantileThresholdModel] with
  ScoreQuantileThresholdParams with
  ParamsServices with
  CustomLogging with
  DefaultParamsWritable
{
  def this() = this(Identifiable.randomUID("quantile_threshold"))

  override def copy(extra: ParamMap): Estimator[ScoreQuantileThresholdModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def fit(dataset: Dataset[_]): ScoreQuantileThresholdModel = {
    // stratification on groups, sampling, fit model for each group ...
    import ScoreQuantileThresholdModel.json
    logInfo("Fit models ...")
    logDebug(s"with params:\n${json(params.map(explain))}")

    validateClassesPriorLabels()
    validateInputColumns(dataset.schema)
    require(getSampleSize > 0, s"Sample size must be > 0, got `${getSampleSize}`")

    val configs: List[(String, models.ScoreQuantileThresholdConfig)] = {

      // stratification and sampling
      val group_score: DataFrame = {
        val cfg = StratifiedSampler.SelectGroupScoreSampleConfig(
          sampleRandomSeed = if (isDefinedSampleRandomSeed) getSampleRandomSeed else Double.NaN,
          sampleSize = getSampleSize,
          groupColumnsList = getGroupColumns.toList,
          inputColName = getInputCol,
          inputCastType = "double",
          validInputExpr = "score is not null and not isnan(score)",
          cacheFunction = cacheFunction
        )

        // Dataset[(group, score)], cached
        StratifiedSampler.getGroupScoreSample(dataset, cfg)
      }

      // distributed fit, (group, message, model)
      val group_model: Array[(String, String, Option[models.ScoreQuantileThresholdConfig])] = ScoreQuantileThresholdEstimator
        .distributedFit(group_score, getPriorValues)
        .collect()

      group_score.unpersist() // TODO: replace cacheFunction with cacheService, it should have 2 methods: cache, uncache

      // report errors and project models
      group_model.toList.flatMap {
        case (group, msg, Some(cfg)) =>
          if (msg.nonEmpty) logWarning(s"group `$group`, fit problems: $msg")
          Some((group, cfg))
        case (group, msg, None) =>
          logWarning(s"group `${group}`, fit has failed: $msg")
          None
      }
    }

    logInfo(s"Fit completed, models count: ${configs.length}")
    logDebug(s"Models data:\n${json(configs)}")
    if (configs.isEmpty) logWarning("Fit should produce at least one model, 0 models fitted")

    copyValues(new ScoreQuantileThresholdModel(uid, configs)).setParent(this)
  }
}

object ScoreQuantileThresholdEstimator {

  def distributedFit(group_score: DataFrame, prior: Array[Double]): Dataset[(String, String, Option[models.ScoreQuantileThresholdConfig])] = {
    import group_score.sparkSession.implicits._

    val fitOnGroup: (String, Iterator[(String, Double)]) => (String, String, Option[models.ScoreQuantileThresholdConfig]) =
      (group, rows) => {
        val res = fit(rows.map(_._2), prior)
        (group, res.msg, res.cfg)
      }

    group_score.as[(String, Double)]
      .groupByKey { case (group, _) => group }
      .mapGroups(fitOnGroup)
  }

  /**
    * Return error message (or empty string) and fitted model parameters
    */
  def fit(scoreRaw: Iterator[Double], prior: Array[Double]): FitResult = Try {

    val xs: Array[Double] = scoreRaw.toArray

    val msg: String = {
      if (xs.length < prior.length) "Fitting quantiles to scores vector xs, xs.length < classes.length"
      else ""
    }

    FitResult(msg, Some(
      models.ScoreQuantileThreshold.fit(xs, prior)
    ))
  } match {
    case Success(res) => res
    case Failure(err) => FitResult(err.getMessage, None)
  }

  case class FitResult(msg: String, cfg: Option[models.ScoreQuantileThresholdConfig])

}
