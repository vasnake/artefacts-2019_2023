/**
 * Created by vasnake@gmail.com on 2024-08-02
 */
package com.github.vasnake.spark.ml.transformer

import org.apache.spark.SparkFiles
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.util.{Failure, Success, Try}

import com.github.vasnake.`etl-core`.GroupedFeatures
import com.github.vasnake.`ml-models`.complex._
import com.github.vasnake.spark.features.vector.FeaturesRowDecoder
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.json.JsonToolbox
import com.github.vasnake.spark.io.HDFSFileToolbox
import com.github.vasnake.json.read.{ModelConfig => ModelsConfigJson}

/**
  * APPLY stage: load batch of models from models descriptions and apply that batch
  * to each partition of input dataset (call model.transform(row) for each model and each row of the dataset.
  * You can consider this as an 'explode' behavior.
  *
  * Complete config consists of some index (directory) in form of (key -> value) map,
  * json files and possibly pmml files.
  * All those files should be created by external process during LEARN stage.
  *
  * @param uid spark.ml pipeline stage id
  */
class ApplyModelsTransformer(override val uid: String) extends Transformer with DefaultParamsWritable {
  // TODO: consider converting 'transformer' to 'model'; that model produced by 'estimator' 'fit' method.
  // Only fit doesn't actually fit model to dataset but load model (batch of models) from storage.
  // In that case we could eliminate 'init' and lazy config loader in transformer code.
  // Or, you can use 'load' interface to perform 'init' functionality.

  def this() = this(Identifiable.randomUID("apply_models"))

  /**
   * transformer parameter: list of models encoded to string by custom encoder
   */
  final val models = new Param[String](this, "models", "List of models descriptions")
  setDefault(models, "")
  def setModels(value: String): this.type = set(models, value)
  def getModels: String = getOrDefault(models)

  /**
   * transformer parameter: df columns to keep
   */
  final val keep_columns = new Param[String](this, "keep_columns", "List of columns names to keep during transformation")
  setDefault(keep_columns, "")
  def setKeepColumns(value: String): this.type  = set(keep_columns, value)
  def getKeepColumns: String = getOrDefault(keep_columns)

  // actual model parameters
  @transient protected lazy val config: ApplyModelsTransformerConfig = buildConfig()

  /**
   * API, load models from files (described in `models` parameter) and check that all things are OK.
   *
   * @return true if models are loaded, list not empty and all is OK, false otherwise
   */
  def initialize(): Boolean = {
    // TODO: add check for models count: models.size == declaredModelsCount
    config.models.nonEmpty && config.models.forall(_.isOK)
  }

  import ApplyModelsTransformer._

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = StructType(
    {
      val keepFields = config.keepColumns.map(colName => schema(colName))
      keepFields ++ predefinedScoringFields
    }
  )

  override def transform(ds: Dataset[_]): DataFrame = {
    val spark = ds.sparkSession
    val keepColsNames = config.keepColumns
    val modelObjects = config.models.filter(_.isOK)

    // all features that we need (GF: Grouped Features)
    val mergedGFs = GroupedFeatures.mergeGroupedFeatures(
      modelObjects.map(_.groupedFeatures)
    )

    // subset of input columns
    val inputDF = {
      val cols: Seq[String] = keepColsNames ++ mergedGFs.groups.map(_.name)
      ds.select(cols.head, cols.tail : _*)
    }

    val inputSchema = inputDF.schema
    val outputSchema = transformSchema(inputSchema)

    logInfo("broadcasting data ...")
    val broadcasted = {
      // array of indices for each model
      val featuresIndices = modelObjects.map(model =>
        GroupedFeatures.computeFeaturesIndices(mergedGFs, model.groupedFeatures)
      )

      val cfg = ApplyModelsDistributedConfig(
        featuresFromRow = FeaturesRowDecoder.apply(inputSchema, mergedGFs), // row decoders
        models_featuresIdx = modelObjects.zip(featuresIndices),
        keepColumnsIndices = keepColsNames.map(inputSchema.fieldIndex)
      )

      spark.sparkContext.broadcast(cfg)
    }

    // each row produce N rows, where N = models.size
    logInfo("applying models ...")
    applyModels(inputDF, outputSchema, broadcasted)
  }

  /**
   * Parse transformer parameters, load models, build config object
   *
   * @return config object
   */
  protected def buildConfig(): ApplyModelsTransformerConfig = {
    val keepColumns: Seq[String] = DescriptionTools.extractKeepColumns(getKeepColumns)
    logInfo(s"Columns to keep: <${getKeepColumns}>, decoded as [${keepColumns.mkString("'", "', '", "'")}]")

    val modelsList: Seq[ComplexMLModel] = {
      logInfo(s"Decoding models description: <${getModels}> ...")
      val descriptions: Seq[ModelDescription] = DescriptionTools.extractModelsDescriptions(getModels)
      logInfo(s"Parsed ${descriptions.size} models descriptions")

      descriptions.map(descr => {
        logInfo(s"Loading model from description <${descr}> ...")
        val model = loadModel(descr)
        logDebug(s"Model loaded: <${model.toString}>.")

        model
      })
    }
    logInfo(s"Loaded ${modelsList.count(m => m.isOK)} valid models")

    ApplyModelsTransformerConfig(modelsList, keepColumns)
  }

  /**
   * Find described model, call model data loading.
   * Keeping this method here provides the ability to test transformer with mock models.
   *
   * @param description model metadata, should contain known `model_type` parameter
   * @return model object
   */
  protected def loadModel(description: ModelDescription): ComplexMLModel = {
    // InvalidModel chosen against Option[Model] for `isOK` attribute sake. Probably wrong solution.

    val modelType: Option[String] = DescriptionTools.getModelType(description)
    logInfo(s"model type: '${modelType}'")

    def unbox(loaded: Try[ComplexMLModel]): ComplexMLModel = loaded match {
      case Success(m) => m
      case Failure(err) =>
        logError(s"model loading failed, m. description: <${description}>; e. message: <${err}>")
        InvalidModel(description, err.getMessage)
    }

    // TODO: should be factored out as registry/factory; you can drop BaseLineDummy then
    modelType match {
      case Some("ScoringCustomLogRegSA") => unbox(loadScoreAudienceModel(description))
      case Some("LalTfidfScaledSgdcPLSA") => unbox(loadLalTfidfScaledSgdcModel(description))
      case Some("LalBinarizedMultinomialNbPLSA") => unbox(loadLalBinarizedMultinomialNbModel(description))
      case Some("BaseLineDummy") => BaseLineModel(description) // n.b. not a best way to facilitate it tests
      case _ =>
        logError(s"model loading failed, unknown model type '${modelType}'; description '${description}'")
        InvalidModel(description, "unknown model type")
    }
  }

}

object ApplyModelsTransformer extends DefaultParamsReadable[ApplyModelsTransformer] {

  // TODO: consider replacing this with combination of interfaces and adapters + 'required schema'
  private val predefinedScoringFields = Seq(
    StructField("score", DataTypes.DoubleType),
    StructField("scores_raw", DataTypes.createArrayType(DataTypes.DoubleType)),
    StructField("scores_trf", DataTypes.createArrayType(DataTypes.DoubleType)),
    StructField("audience_name", DataTypes.StringType),
    StructField("category", DataTypes.StringType)
  )

  /**
   * Factory
   * @param modelsList models description list, encoded
   * @param keepColumnsList columns names to keep during transformation, encoded
   * @return transformer object
   */
  def apply(modelsList: String, keepColumnsList: String): ApplyModelsTransformer = {
    val trf = new ApplyModelsTransformer()

    trf
      .setModels(modelsList)
      .setKeepColumns(keepColumnsList)
  }

  def applyModels(df: DataFrame, outSchema: StructType, broadcasted: Broadcast[ApplyModelsDistributedConfig]): DataFrame =
    df.mapPartitions(
      applyModelsToIter(_, broadcasted)
    )(RowEncoder(outSchema))

  private def applyModelsToIter(rows: Iterator[Row], broadcasted: Broadcast[ApplyModelsDistributedConfig]): Iterator[Row] =
    rows.flatMap(
      applyModelsToRow(_, broadcasted.value)
    )

  private def applyModelsToRow(row: Row, cfg: ApplyModelsDistributedConfig): Seq[Row] = {
    import GroupedFeatures.{sliceFeaturesVector => slice}
    val keepValues = cfg.keepColumnsIndices.map(idx => row(idx))
    val allFeaturesArray = cfg.featuresFromRow.decode(row)

    // one row => models.size rows
    cfg.models_featuresIdx.map { case (model, featuresIndices) =>
      Row.fromSeq(
        keepValues ++ model.apply(slice(allFeaturesArray, featuresIndices))
      )
    }
  }

  type ModelDescription = Map[String, String]

  object DescriptionTools {
    import StringToolbox._

    // TODO: consider replacing this dumb solution with old plain json codec

    // separators for text in `models` parameter
    val CFG_MODELS_SEPARATOR = ";" // list of models: model_descr1;model_descr2;...
    val CFG_MODEL_PARAMETERS_SEPARATOR = "," // list of pairs: k1->v1,k2->v2,...
    val CFG_MODEL_KV_SEPARATOR = "->" // pair: a->b
    val CFG_COLUMNS_SEPARATOR = "," // list of names: col1,col2,...

    val CFG_MODEL_TYPE_KEY = "model_type" // fixed model type name is stored under that key

    def getModelType(descr: ModelDescription): Option[String] = descr.get(CFG_MODEL_TYPE_KEY)

    // decode transformer parameter to list of models descriptions
    def extractModelsDescriptions(text: String): Seq[ModelDescription] = {
      implicit val sep: Separators = Separators(CFG_MODELS_SEPARATOR)

      text.splitTrim.map(extractModelDescription)
    }

    private def extractModelDescription(text: String): ModelDescription = {
      implicit val sep: Separators = Separators(
        CFG_MODEL_PARAMETERS_SEPARATOR,
        Some(Separators(CFG_MODEL_KV_SEPARATOR))
      )

      text.parseMap
    }

    def extractKeepColumns(text: String): Seq[String] = {
      implicit val sep: Separators = Separators(CFG_COLUMNS_SEPARATOR)

      text.splitTrim
    }

    // pack <-> extract

    def packModelsDescriptions(descrs: Seq[ModelDescription]): String =
      descrs.map(packModelDescription).mkString(CFG_MODELS_SEPARATOR)

    private def packModelDescription(descr: ModelDescription): String = {
      descr.toList.map { case (k, v) => s"${k}${DescriptionTools.CFG_MODEL_KV_SEPARATOR}${v}" }
        .sorted
        .mkString(DescriptionTools.CFG_MODEL_PARAMETERS_SEPARATOR)
    }

    def packKeepColumns(cols: Seq[String]): String =
      cols.mkString(CFG_COLUMNS_SEPARATOR)

  }

  def loadScoreAudienceModel(description: ModelDescription): Try[ScoreAudienceModel] = Try {
      val audienceName = description("audience_name")
      val json = loadJsonFromFile(description("model_path"))

      val predictor = ModelsConfigJson.loadScoreAudiencePredictorFromJson(json)

      ScoreAudienceModel(audienceName, predictor)
  }

  def loadLalBinarizedMultinomialNbModel(description: ModelDescription): Try[LalBinarizedMultinomialNbModel] = Try {
    val audienceName = description("audience_name")
    val equalizerSelector = description("equalizer_selector")

    val groupedFeatures = ModelsConfigJson.loadGroupedFeaturesFromJson(
      loadJsonFromFile(description("features_path"))
    )

    val config = ModelsConfigJson.loadLalBinarizedMultinomialNbModelConfigFromJson(
      loadJsonFromFile(description("model_repr_path"))
    )

    LalBinarizedMultinomialNbModel(config, groupedFeatures, audienceName, equalizerSelector)
  }

  def loadLalTfidfScaledSgdcModel(description: ModelDescription): Try[LalTfidfScaledSgdcModel] = Try {
    val audienceName = description("audience_name")
    val equalizerSelector = description("equalizer_selector")

    val groupedFeatures = ModelsConfigJson.loadGroupedFeaturesFromJson(
      loadJsonFromFile(description("features_path"))
    )

    val config = ModelsConfigJson.loadLalTfidfScaledSgdcModelConfigFromJson(
      loadJsonFromFile(description("model_repr_path"))
    )

    LalTfidfScaledSgdcModel(config, groupedFeatures, audienceName, equalizerSelector)
  }

  private def loadJsonFromFile(path: String) = JsonToolbox.parseJson(readTextFile(path))

  /**
    * Try to load file from SparkFiles local path; if no good, load file from local path; finally try HDFS
    *
    * @return file body or exception
    */
  private def readTextFile(path: String): String = {
    import org.slf4j.LoggerFactory
    val log = LoggerFactory.getLogger(getClass)
    log.info(s"loading file '${path}' ...")

    val trySparkFiles = Try {
      val sparkFilesPath = {
        val lastDirName = FileToolbox.getPathBasename(
          FileToolbox.getPathDirname(path)
        )

        FileToolbox.joinPath(
          SparkFiles.get(lastDirName),
          FileToolbox.getPathBasename(path)
        )
      }

      log.debug(s"loading local text file '${sparkFilesPath}' ...")
      FileToolbox.loadTextFile(sparkFilesPath)
    }

    val tryLocalPath = trySparkFiles.recoverWith { case err =>
      log.warn(s"loading text file from SparkFiles cache failed, exception: ${err}")
      Try {
        log.debug(s"loading local text file '${path}' ...")
        FileToolbox.loadTextFile(path)
    }}

    val tryHdfs = tryLocalPath.recover { case err =>
      log.warn(s"loading local text file failed, exception: ${err}")
      log.debug(s"loading text file from HDFS '${path}' ...")
      HDFSFileToolbox.loadTextFile(path)
    }

    tryHdfs.get // or die
  }

}

case class ApplyModelsTransformerConfig
(
  models: Seq[ComplexMLModel],
  keepColumns: Seq[String]
)

case class ApplyModelsDistributedConfig
(
  featuresFromRow: FeaturesRowDecoder,
  models_featuresIdx: Seq[(ComplexMLModel, Array[Int])],
  keepColumnsIndices: Seq[Int]
)
