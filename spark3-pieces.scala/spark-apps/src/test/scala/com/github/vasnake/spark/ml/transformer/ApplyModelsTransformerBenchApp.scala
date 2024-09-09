/** Created by vasnake@gmail.com on 2024-08-13
  */
package com.github.vasnake.spark.ml.transformer

import com.github.vasnake.`etl-core`.GroupedFeatures
import com.github.vasnake.`ml-models`.complex._
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.json.JsonToolbox
import com.github.vasnake.json.read.ModelConfig
import org.apache.spark.vasnake.app.SparkApp

import org.apache.spark.sql
import sql._

import org.apache.spark.storage.StorageLevel

import org.slf4j.LoggerFactory

/** {{{
  * Run scala-apply benchmarks. This code was useful during 'optimisation' stage of development.
  * see run-job-dm-6915.sh
  *
  * }}}
  */
object ApplyModelsTransformerBenchApp extends SparkApp {
  class Config(args: Iterable[String]) {
    private val cfg = {
      import StringToolbox._

      // `;` parameters separator, if you have more than one in one arg
      // `=` key=value separator for each parameter
      implicit val separators = Separators(";", Some(Separators("=")))

      for {
        line <- args
        kv <- line.parseMap
      } yield kv
    }.toMap

    // source dataframe, parquet
    val inputPath: String = cfg("featuresDataset")

    // source explode factor
    val explodeFactor: Int = cfg.getOrElse("explodeFactor", "1").toInt

    // model data file
    val groupedFeaturesPath: String = cfg("featuresIndex")

    // model config
    val tfidfScaledSgdcModelPath: Option[String] = cfg.get("tfidfScaledSgdcModelRepr")

    // model config
    val binarizedMultinomialNbModelPath: Option[String] = cfg.get("binarizedMultinomialNbModelRepr")

    // source repartition parameter
    val numPartitions: Int = cfg.getOrElse("numPartitions", "16").toInt

    // do scalameter loop?
    val scalameter: String = cfg.getOrElse("scalameter", "off")

    // run transformation `repeat` times
    val repeat: Int = cfg.getOrElse("repeat", "1").toInt

    override def toString: String = cfg.toList.map(kv => s"${kv._1}=${kv._2}").sorted.mkString(";")
  }

  case class DummyModel(
    groupedFeatures: GroupedFeatures,
    audienceName: String,
    mulFactor: Double
  ) extends ComplexMLModel {
    override def isOK: Boolean = true

    override def apply(features: Array[Double]): Seq[Any] = {
      val scores_raw: Array[Double] = Array(features.filter(!_.isNaN).sum * mulFactor)
      val scores: Array[Double] = scores_raw.map(_ / features.length)

      Seq(
        scores(0), // equalized score value
        scores_raw, // raw scores array
        scores, // equalized scores array
        audienceName, // project id (target)
        category
      )
    }

    private val category = "positive"
  }

  run {
    case args =>
      implicit spark =>
        println(s"ApplyModelsJob arguments: <${args.mkString(";")}>")
        applyModels(spark, new Config(args))
  }

  def applyModels(spark: SparkSession, config: Config): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    log.info(s"parsed arguments: <$config>")

    // model data
    val groupedFeatures = ModelConfig.loadGroupedFeaturesFromJson(
      JsonToolbox.parseJson(FileToolbox.loadTextFile(config.groupedFeaturesPath))
    )
    log.info(s"loaded features index, features: ${groupedFeatures.featuresSize}")

    // model config, optional
    val tfidfScaledSgdcModelCfg = config
      .tfidfScaledSgdcModelPath
      .map(path =>
        ModelConfig.loadLalTfidfScaledSgdcModelConfigFromJson(
          JsonToolbox.parseJson(FileToolbox.loadTextFile(path))
        )
      )
    if (tfidfScaledSgdcModelCfg.isDefined) log.info("loaded LalTfidfScaledSgdcModel config")

    // model config, optional
    val binarizedMultinomialNbModelCfg = config
      .binarizedMultinomialNbModelPath
      .map(path =>
        ModelConfig.loadLalBinarizedMultinomialNbModelConfigFromJson(
          JsonToolbox.parseJson(FileToolbox.loadTextFile(path))
        )
      )
    if (binarizedMultinomialNbModelCfg.isDefined)
      log.info("loaded LalBinarizedMultinomialNbModel config")

    // models to apply
    val models: Seq[ComplexMLModel] = tfidfScaledSgdcModelCfg
      .map(cfg =>
        Seq(
          LalTfidfScaledSgdcModel(cfg, groupedFeatures, "tfidf1", "OKID"),
          LalTfidfScaledSgdcModel(cfg, groupedFeatures, "tfidf2", "VKID"),
          LalTfidfScaledSgdcModel(cfg, groupedFeatures, "tfidf3", "EMAIL")
        )
      )
      .getOrElse(
        binarizedMultinomialNbModelCfg
          .map(cfg =>
            Seq(
              LalBinarizedMultinomialNbModel(cfg, groupedFeatures, "binmnb1", "OKID"),
              LalBinarizedMultinomialNbModel(cfg, groupedFeatures, "binmnb2", "VKID"),
              LalBinarizedMultinomialNbModel(cfg, groupedFeatures, "binmnb3", "EMAIL")
            )
          )
          .getOrElse(
            Seq(
              DummyModel(groupedFeatures, "dm1", 0.1),
              DummyModel(groupedFeatures, "dm2", 0.01),
              DummyModel(groupedFeatures, "dm3", 0.001)
            )
          )
      )

    val transformer = getTransformer(models, keepColumns = Seq("uid"))
    log.info(s"created transformer ${transformer}")

    // source dataframe
    val input = {
      val df = spark.read.parquet(config.inputPath).orderBy("uid")
      val exploded =
        if (config.explodeFactor > 1) {
          log.info("explode input ...")
          explode(df, config.explodeFactor)
        }
        else df

      exploded.repartition(config.numPartitions).persist(StorageLevel.MEMORY_ONLY)
    }
    log.info(s"input materialized, rows: ${input.count()}")

    // run
    (1 to config.repeat).foreach(round => spark.time(transform(input, transformer, round)))

    if (config.scalameter == "on") {
      val timeBench = {
        import org.scalameter._
        org
          .scalameter
          .config(
            Key.exec.minWarmupRuns -> 1,
            Key.exec.maxWarmupRuns -> 2,
            Key.exec.benchRuns -> 3,
            Key.verbose -> false
          )
          .withWarmer(new Warmer.Default)
      }
      log.info(s"scalameter time bench created, measuring ...")
      val benchResult = timeBench measure transform(input, transformer, 0)
      log.info(s"transform time: ${benchResult}")
    }
  }

  private def transform(
    input: DataFrame,
    transformer: ApplyModelsTransformer,
    round: Int
  ): Unit = {
    if (!transformer.initialize()) sys.error("can't initialize scala-apply transformer")
    val output: DataFrame = transformer.transform(input)

    // materialize methods
    // println(output.count())
    // output.describe().show(truncate = false)

    // WARNING: iterators are lazy!
    // import input.sparkSession.implicits._
    // output.mapPartitions(iter => Iterator(MurmurHash3.unorderedHash(iter.map(_.hashCode())))).collect()

    // should be the fastest materializer:
    // output.foreachPartition(rows => println(s"partition hash: ${MurmurHash3.unorderedHash(rows.map(_.hashCode))}"))

    // 0.5 sec faster than hash
    val materialize: (Iterator[sql.Row] => Unit) =
      rows => println(s"partition magic number: ${rows.map(_.hashCode.toDouble).sum}")

    output.foreachPartition(materialize)

    // if (round == 1) output.explain(extended = true)
  }

  private def getTransformer(
    grinderModels: Seq[ComplexMLModel],
    keepColumns: Seq[String] = Seq.empty
  ) =
    new ApplyModelsTransformer() {
      override def buildConfig(): ApplyModelsTransformerConfig =
        ApplyModelsTransformerConfig(grinderModels, keepColumns)
    }

  private def explode(df: DataFrame, factor: Int): DataFrame = {
    implicit val encoder: Encoder[Row] = sql.Encoders.row(df.schema)
    df.flatMap(row => for { _ <- 1 to factor } yield row.copy())
    // outdf.repartition(1).write.mode("overwrite").option("compression", "gzip").parquet(path)
  }
}
