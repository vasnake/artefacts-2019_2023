/**
 * Created by vasnake@gmail.com on 2024-07-25
 */
package com.github.vasnake.spark.dataset.transform

import com.github.vasnake.spark.io.{Logging => CustomLogging}
import com.github.vasnake.common.num.{FastMath => fm}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.DataTypes

/**
 * Stratification and sampling example
 *
 * <pre>

      val groupScoreSample: DataFrame = {
        val cfg = StratifiedSampling.SelectGroupScoreSampleConfig(
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
        StratifiedSampling.getGroupScoreSample(dataset, cfg)
      }
 * </pre>
 *
 */
object StratifiedSampler extends GroupingColumnsServices with CustomLogging {

  case class SelectGroupScoreSampleConfig(
                                           sampleRandomSeed: Double = Double.NaN,
                                           sampleSize: Long = 100000,
                                           groupColumnsList: List[String],
                                           inputColName: String = "score",
                                           inputCastType: String = "double",
                                           validInputExpr: String = "score is not null and not isnan(score)",
                                           cacheFunction: Option[DataFrame => DataFrame]
                                         )
  // TODO: make it more generic: list of grouping columns, list of data columns, cacheService(persist, unpersist).
  // TODO: debug performance issues.

  def getGroupScoreSample(dataset: Dataset[_], cfg: SelectGroupScoreSampleConfig): DataFrame = {
    implicit val spark: SparkSession = dataset.sparkSession
    import spark.implicits._

    // project, filter and cache (option) input data
    val group_score: DataFrame = {
      // select
      val gs = {
        val df = selectGroupAndScore(dataset.toDF(), cfg.groupColumnsList, cfg.inputColName, cfg.inputCastType)
        // cut off old names from execution plan, bad for performance
        val group_score = dataset.sparkSession.createDataFrame(df.rdd, df.schema)

        group_score
          .na.drop("any")
          .filter(cfg.validInputExpr)
      }

      // cache
      cfg.cacheFunction.map(cache => {
        val cached = cache(gs)
        // try to fix cache quirks
        logInfo("cache function defined, materializing group_score dataset ...")
        val cnt = cached.count()
        logDebug(s"total rows: ${cnt}")

        cached
      }).getOrElse({
        logInfo("cache function undefined, sampling could be slow")

        gs // non-cached
      })
    }

    // get sample, cached
    val group_score_sample: DataFrame = {
      // list of pairs (group_name, items_count)
      val group_count: Array[(String, Long)] = group_score.groupBy("group").count()
        .as[(String, Long)]
        .collect()
      logDebug(s"stratification groups (group, size):\n${json(group_count)}")

      sample(
        ds = group_score,
        group_size = group_count,
        sampleSize = cfg.sampleSize,
        seed = cfg.sampleRandomSeed
      )
    }.cache() // group_score_sample cached

    // materialize sample, free cached source
    logDebug(s"sample rows: ${group_score_sample.count()}")
    group_score.unpersist() // TODO: should be something like: CacheService.unpersist, from object given in parameter

    group_score_sample
  }

  private def selectGroupAndScore(
                           dataset: DataFrame,
                           groupColumnsList: Seq[String],
                           inputColName: String,
                           inputCastType: String
                          ): DataFrame = {

    addTempGroupingColumn(dataset, groupColumnsList)
      .selectExpr(TEMP_GROUP_COLUMN, s"cast(${inputColName} as ${inputCastType})") // TODO: cast should be optional
      .toDF("group", "score") // magic names
  }

  private def sample(
              ds: DataFrame,
              group_size: Array[(String, Long)],
              sampleSize: Long,
              seed: Double = Double.NaN
            ): DataFrame = {

    val maxRows: Double = sampleSize.toDouble
    val actualSeed: Long = if (seed.isNaN) System.currentTimeMillis() else seed.toLong
    logDebug(s"sample seed: ${actualSeed}")

    val fractions: Map[String, Double] = group_size.map { case (group, count) =>
      group -> fm.min(1.0, maxRows / count)
    }.toMap
    logDebug(s"sample fractions:\n${json(fractions)}")

    ds.toDF("group", "score")
      .stat
      .sampleBy("group", fractions, actualSeed)
  }

  // TODO: don't use generics
  import com.github.vasnake.jsonish.PrettyPrint._
  import com.github.vasnake.jsonish.PrettyPrint.implicits.{jarray, jmap}
  private def json(x: Array[_]): String = jsonify(x)
  private def json(x: Map[_, _]): String = jsonify(x)

}

/**
  * Add/drop column with concatenated stratification values, to form 'group name'.
 * Group name must be constructed using one and only one format rule, therefore: this service.
  */
trait GroupingColumnsServices {

  val TEMP_GROUP_COLUMN = "_temp_concatenated_grouping_columns_" // TODO: name should be calculated dynamically, based on DF schema
  // see com.github.vasnake.spark.dataset.Helpers.getNewTempColumnName

  // add column that contains 'group' name in specific format
  def addTempGroupingColumn(
                             dataset: Dataset[_],
                             groupingColumns: Seq[String],
                             groupColname: String = TEMP_GROUP_COLUMN
                           ): DataFrame = {
    import org.apache.spark.sql.{functions => sf}

    dataset.withColumn(
      groupColname,
      sf.concat_ws(
        "/",
        (sf.lit('g') +: groupingColumns.map(colname =>
          sf.coalesce(sf.col(colname), sf.lit('-')).cast(DataTypes.StringType)
        )): _*)
    )
  }

  def dropTempGroupingColumn(df: DataFrame, groupColName: String = TEMP_GROUP_COLUMN): DataFrame = df.drop(groupColName)

  def addGroupToGroupingKey(key: String, group: String): String = s"${key}/${if (group.isEmpty) "-" else group}"
}
