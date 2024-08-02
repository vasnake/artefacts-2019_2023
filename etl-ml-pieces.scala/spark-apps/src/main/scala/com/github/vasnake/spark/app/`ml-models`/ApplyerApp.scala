/**
 * Created by vasnake@gmail.com on 2024-08-02
 */
package com.github.vasnake.spark.app.`ml-models`

import org.apache.spark.sql.DataFrame

import scala.util.Try
import com.beust.jcommander

import com.github.vasnake.spark.app.SparkSubmitApp
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.spark.ml.transformer.ApplyModelsTransformer

object ApplyerApp extends SparkSubmitApp(CmdLineParams) {
  type AMT = ApplyModelsTransformer

  logger.info(s"Loading transformation config `${CmdLineParams.transformation_config}` ...")
  private val trfCfg: TransformationConfig = {
    import StringToolbox._
    import DefaultB64Mapping.extraCharsMapping

    val jsonText = CmdLineParams.transformation_config.b64Decode
    logger.info(s"config json: `${jsonText}`")
    TransformationConfig.parseJson(jsonText)
  }
  logger.info(s"Config loaded: `${trfCfg}`")

  // load source; init transformer, call transform, write result
  var res = for {
    srcDF <- loadSource(trfCfg.sourcePath)
    trf <- createTransformer(trfCfg.models, trfCfg.keepColumns)
    trgDF <- Try(trf.transform(srcDF))
    written <- writeTarget(trgDF, trfCfg.targetPath)
  } yield written

  reportResult(res)

  def loadSource(path: String): Try[DataFrame] = ???
  def createTransformer(modelsList: String, keepColumnsList: String): Try[AMT] = ???
  def writeTarget(df: DataFrame, path: String): Try[Unit] = ???
  def reportResult(res: Try[_]): Unit = ???
}

object CmdLineParams {
  import jcommander.Parameter

  @Parameter(names = Array("--transformation-config"), required = true, description = "Job config, json text encoded base64")
  var transformation_config: String = _ // models_list, keep_columns_list, source_load_path, target_write_path

  override def toString: String =
    s"""CmdLineParams(transformation_config="$transformation_config""""

}

/**
 * Models_list, keep_columns_list, load_path, write_path
 * @param sourcePath
 * @param targetPath
 * @param models
 * @param keepColumns
 */
case class TransformationConfig(sourcePath: String, targetPath: String, models: String, keepColumns: String)
object TransformationConfig {
  def parseJson(jsonText: String): TransformationConfig = {
    ???
  }
}
