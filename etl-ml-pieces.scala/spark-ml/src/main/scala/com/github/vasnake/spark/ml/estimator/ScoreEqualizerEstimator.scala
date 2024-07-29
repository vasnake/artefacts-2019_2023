/**
 * Created by vasnake@gmail.com on 2024-07-29
 */
package com.github.vasnake.spark.ml.estimator

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.github.vasnake.spark.ml.shared._
import com.github.vasnake.spark.ml.model.ScoreEqualizerModel
import com.github.vasnake.`ml-models`.{complex => models}
import com.github.vasnake.spark.dataset.transform.{GroupingColumnsServices, StratifiedSampler}
import com.github.vasnake.spark.io.{Logging => CustomLogging}
import com.github.vasnake.common.num.{FastMath => fm}

/**
  * Stratified equalizer, Spark.ml estimator: implementation of the `fit` method.
  *
  * Use `fit` method to produce trained equalizer model (transformer), using sample of input DataFrame.
  * Because fitting involves materialization of input data, you should persist input DataFrame before calling `fit`.
  *
  * Stratification: for different groups of records different equalizers could be fitted using `groupColumns` parameter.
  */
class ScoreEqualizerEstimator(override val uid: String) extends Estimator[ScoreEqualizerModel] with
  ScoreEqualizerParams with
  ParamsServices with
  GroupingColumnsServices with
  CustomLogging with
  DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("equalizer"))

  override def copy(extra: ParamMap): Estimator[ScoreEqualizerModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  // TODO: performance optimizations
  override def fit(dataset: Dataset[_]): ScoreEqualizerModel = {
    // with sampling and stratification (grouping columns) one EQ for each group:
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
      val groupScoreSample: DataFrame = {
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
      val group_result: Array[(String, String, Option[models.ScoreEqualizerConfig])] = {
        val rnd = if (isDefinedRandomValue) getRandomValue else Double.NaN
        ScoreEqualizerEstimator
          .distributedFit(groupScoreSample, getMinInputSize, getNoiseValue, getEpsValue, rnd, getNumBins)
          .collect()
      }

      groupScoreSample.unpersist()

      // report errors and project models
      group_result.toList.flatMap {
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

  // invalid data filtered out already
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
        val (msg, eq) = fitScoreEqualizer(rows.map(_._2), minInputSize, noiseValue, epsValue, randomValue, numBins)
        (group, msg, eq)
      }}
  }

  /**
    * Return error message (or empty string) and fitted model parameters
    */
  def fitScoreEqualizer(
                         scoreRaw: Iterator[Double],
                         minInputSize: Int,
                         noiseValue: Double,
                         epsValue: Double,
                         randomValue: Double,
                         numBins: Int
                       ): (String, Option[models.ScoreEqualizerConfig]) = Try {

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

      "'X' contains single unique value, so equalization cannot be done properly"
    } else ""

    val config = models.ScoreEqualizerConfig(
      noise = noiseValue,
      eps = epsValue,
      random_value = if (randomValue.isNaN) None else Some(randomValue),
      minval = minval,
      maxval = maxval,
      // TODO: we don't need cdf here, ScoreEqualizer should be split on two transformers: normalizer and equalizer
      coefficients = (0 to 3).map(_ => Array(0.0)).toArray,
      intervals = Array(0.0, 1.0)
    )

    // scale, add noise, stretch
    val stretchedScore: Array[Double] = {
      val eq = models.ScoreEqualizer(config)

      arr.map(x => eq.scaleAddNoiseAndStretch(x))
    }

    // PCHIP spline intervals and coefficients
    val cdf: CDFCoefficients = ScoreEqualizerEstimator.fitCDF(stretchedScore, numBins)
    // (Piecewise Cubic Hermite Interpolating Polynomial; Cumulative Distribution Function)

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

    // TODO: move this function to ScoreEqualizer.fit method

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
    // TODO: collect stat and metrics, send it to driver
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

}

case class CDFCoefficients(intervals: Array[Double], coefficients: Array[Array[Double]])
