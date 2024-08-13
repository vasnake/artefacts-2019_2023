/** Created by vasnake@gmail.com on 2024-08-02
  */
package com.github.vasnake.spark.app.`ml-models`

import scala.util._

import com.beust.jcommander
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.json.JsonToolbox
import com.github.vasnake.spark.app.SparkSubmitApp
import com.github.vasnake.spark.ml.transformer.ApplyModelsTransformer
import org.apache.spark.sql.DataFrame

object ApplyerApp extends SparkSubmitApp(CmdLineParams) {
  logger.info(s"Loading applyer config `${CmdLineParams.transformer_config}` ...")

  private val cfg: ApplyerConfig = {
    import StringToolbox._
    import DefaultB64Mapping.extraCharsMapping

    val jsonText = CmdLineParams.transformer_config.b64Decode
    logger.debug(s"config json: `${jsonText}`")

    ApplyerConfig.parseJson(jsonText)
  }

  logger.info(s"Config loaded: `${cfg}`")

  var res = for {
    srcDF <- loadSource(cfg.sourcePath)
    trf <- createTransformer(cfg.models, cfg.keepColumns)
    trgDF <- Try(trf.transform(srcDF))
    written <- writeTarget(trgDF, cfg.targetPath)
  } yield written

  reportResult(res)

  private def loadSource(path: String): Try[DataFrame] = Try {
    spark.read.parquet(path)
  }

  private def createTransformer(modelsList: String, keepColumnsList: String)
    : Try[ApplyModelsTransformer] = Try {
    val trf = ApplyModelsTransformer.apply(modelsList, keepColumnsList)
    val isOK = trf.initialize()
    require(isOK, "Initialization failed, probably config is not valid, see logs for details")

    trf
  }

  private def writeTarget(df: DataFrame, path: String): Try[Unit] = Try {
    df.write
      .mode("overwrite")
      .option("compression", "gzip")
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .parquet(path)
  }

  private def reportResult(res: Try[_]): Unit = res match {
    case Failure(exception) => logger.error("Appyer app: fail", exception)
    case Success(_) => logger.info("Applyer app: success")
  }
}

object CmdLineParams {
  import jcommander.Parameter

  @Parameter(
    names = Array("--transformer-config"),
    required = true,
    description = "Job config, base64-encoded json text",
  )
  var transformer_config: String = _ // models_list, keep_columns_list, source_load_path, target_write_path

  override def toString: String =
    s"""CmdLineParams(transformer_config="$transformer_config""""
}

/** Apply ML models to DF, config: Models_list, keep_columns_list, load_path, write_path
  * @param sourcePath input DataFrame source
  * @param targetPath output DataFrame write path
  * @param models models list, encoded
  * @param keepColumns list of columns names to keep, encoded
  */
case class ApplyerConfig(
  sourcePath: String,
  targetPath: String,
  models: String,
  keepColumns: String,
)

object ApplyerConfig {
  def parseJson(jsonText: String): ApplyerConfig = {
    import JsonToolbox.readObject

    readObject[ApplyerConfig](jsonText)
  }
}
