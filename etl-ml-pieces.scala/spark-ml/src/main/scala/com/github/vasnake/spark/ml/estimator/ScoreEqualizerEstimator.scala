/**
 * Created by vasnake@gmail.com on 2024-07-29
 */
package com.github.vasnake.spark.ml.estimator

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import com.github.vasnake.spark.ml.shared._
import com.github.vasnake.spark.ml.model.ScoreEqualizerModel
import com.github.vasnake.`ml-models`.{complex => models}
import com.github.vasnake.spark.dataset.transform.{StratifiedSampler}
import com.github.vasnake.spark.io.{Logging => CustomLogging}

/**
  * Stratified equalizer, Spark.ml estimator: implementation of the `fit` method.
  *
  * Use `fit` method to produce trained equalizer model (transformer), using sample of input DataFrame.
  * Because fitting involves materialization of input data, you should cache input DataFrame before calling `fit`.
  *
  * Stratification: for different groups of records different equalizers could be fitted using `groupColumns` parameter.
  */
class ScoreEqualizerEstimator(override val uid: String) extends Estimator[ScoreEqualizerModel] with
  ScoreEqualizerParams with
  ParamsServices with
  CustomLogging with
  DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("equalizer"))

  override def copy(extra: ParamMap): Estimator[ScoreEqualizerModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  // TODO: performance optimizations
  override def fit(dataset: Dataset[_]): ScoreEqualizerModel = {
    // with sampling and stratification (grouping columns), fit one EQ for each group:
    // - get groups samples;
    // - group by stratification columns;
    // - for each group: fit named equalizer;
    // - collect all equalizers, pass them to model constructor.
    import ScoreEqualizerModel.json
    logInfo("Fit equalizers ...")
    logDebug(s"with params:\n${json(params.map(explain))}")

    validateInputColumns(dataset.schema)

    val configs: List[(String, models.ScoreEqualizerConfig)] = {

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
      val group_model: Array[(String, String, Option[models.ScoreEqualizerConfig])] = {
        val rnd = if (isDefinedRandomValue) getRandomValue else Double.NaN
        ScoreEqualizerEstimator
          .distributedFit(group_score, getMinInputSize, getNoiseValue, getEpsValue, rnd, getNumBins)
          .collect()
      }

      group_score.unpersist() // TODO: replace cacheFunction to cacheService, it should have 2 methods: cache, uncache

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

    copyValues(new ScoreEqualizerModel(uid, configs)).setParent(this)
  }

}

object ScoreEqualizerEstimator {

  // TODO: try to use known libs for "numpy in java": (nd4j, numsca, breeze)?
  //  https://github.com/botkop/numsca
  //  https://github.com/scalanlp/breeze
  //  https://www.google.ru/m?q=numpy+wrapped+in+java&client=ms-opera-mobile&channel=new&espv=1
  //  https://stackoverflow.com/questions/58352796/numpy-array-computation-slower-than-equivalent-java-code
  //  https://stackoverflow.com/questions/8375558/java-equivalent-for-the-numpy-multi-dimensional-object

  // invalid data filtered out already. return (name, message, model)
  def distributedFit(
                      group_score: DataFrame,
                      minInputSize: Int,
                      noiseValue: Double,
                      epsValue: Double,
                      randomValue: Double,
                      numBins: Int
                    ): Dataset[(String, String, Option[models.ScoreEqualizerConfig])] = {

    val spark = group_score.sparkSession
    import spark.implicits._

    group_score.as[(String, Double)]
      .groupByKey { case (group, _) => group }
      .mapGroups { case (group, rows) => {
        val (msg, eq) = fitOneEqualizer(rows.map(_._2), minInputSize, noiseValue, epsValue, randomValue, numBins)
        (group, msg, eq)
      }}
  }

  // TODO: use case class(msg, optional model)
  /**
    * Return error message (or empty string) and fitted model parameters
    */
  def fitOneEqualizer(
                         scoreRaw: Iterator[Double],
                         minInputSize: Int,
                         noiseValue: Double,
                         epsValue: Double,
                         randomValue: Double,
                         numBins: Int
                       ): (String, Option[models.ScoreEqualizerConfig]) =
    models.ScoreEqualizer.fit(scoreRaw, minInputSize, noiseValue, epsValue, randomValue, numBins)

}
