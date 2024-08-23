/** Created by vasnake@gmail.com on 2024-07-12
  */
package com.github.vasnake.json.read

import com.github.vasnake.`etl-core`._
import com.github.vasnake.`ml-core`.models._
import com.github.vasnake.`ml-models`.complex._
import com.github.vasnake.json.JsonToolbox._
import io.circe.Json

object ModelConfig {
  // TODO: hide Json under toolbox wraps
  def loadSBGroupedEqualizersConfigFromJson(json: Json): SBGroupedTransformerConfig = {
    // TODO: simplify parsers code, apply that pattern to all parsers:
    val equalizerConfig = j2m(json)
    val equalizers = j2m(equalizerConfig("transformers"))

    val configsMap = equalizers.flatMap {
      case (key, json) =>
        key match {
          case name: String => Some(name -> loadScoreEqualizerConfigFromJson(json))
          case _ => None
        }
    }

    SBGroupedTransformerConfig(configsMap)
  }

  def loadBinarizerConfigFromJson(json: Json): BinarizerConfig = {
    val conf = j2m(json)
    val value = conf("threshold").as[Double].fold(decodeErr, v => v)

    BinarizerConfig(threshold = value)
  }

  def loadMultinomialNBConfigFromJson(json: Json): MultinomialNBConfig = {
    val conf = j2m(json)

    MultinomialNBConfig(
      featuresLength = conf("input_length").as[Int].fold(decodeErr, v => v),
      predictLength = conf("predict_length").as[Int].fold(decodeErr, v => v),
      classLogPrior = conf("class_log_prior").as[Seq[Double]].fold(decodeErr, v => v.toArray),
      featureLogProb =
        conf("feature_log_prob").as[Seq[Seq[Double]]].fold(decodeErr, v => v.map(_.toArray).toArray)
    )
  }

  def loadGroupedFeaturesFromJson(json: Json): GroupedFeatures = {
    // field names in their original order
    val keys = json.hcursor.keys.getOrElse(Seq.empty[String])
    val jsonObject = json.asObject.getOrElse(sys.error("malformed grouped features json"))

    val groups = keys.map { name =>
      val indexJson = jsonObject(name).getOrElse(sys.error(s"can't get index for key '${name}'"))

      // you should try list of strings first: not much of numeric arrays in grouped features
      val (index, isInt) = indexJson
        .as[List[String]]
        .fold(
          _ =>
            indexJson
              .as[List[Int]]
              .fold(e => sys.error(e.getMessage()), v => (v.map(_.toString).toArray, true)),
          v => (v.toArray, false)
        )

      FeaturesGroup(name, index, isArray = isInt)
    }

    GroupedFeatures(groups.toSeq)
  }

  def loadImputerConfigFromJson(json: Json): Array[Double] = {
    val ivKey = "imputer_values"
    val jsonObject = json.asObject.getOrElse(sys.error("malformed imputer json"))
    val imputerValuesJson =
      jsonObject(ivKey).getOrElse(sys.error(s"can't get '${ivKey}' from json"))

    val imputerValues = imputerValuesJson
      .as[List[Double]]
      .fold(
        fail => sys.error(s"malformed list of floats, err: ${fail.getMessage()}"),
        lst => lst.toArray
      )

    imputerValues
  }

  def loadGroupedFeaturesTfidfTransformerConfigFromJson(json: Json)
    : GroupedFeaturesTfidfTransformerConfig = {

    def parseConfig(jMap: Map[String, Json]) = {
      val numFeatures =
        jMap("n_features").as[Int].fold(fail => sys.error(fail.getMessage()), v => v)
      val groups = jMap("groups")
        .as[Seq[Seq[Int]]]
        .fold(fail => sys.error(fail.getMessage()), lst => lst.map(_.toArray).toArray)
      val diags = jMap("idf_diags")
        .as[Seq[Seq[Double]]]
        .fold(fail => sys.error(fail.getMessage), lst => lst.map(_.toArray).toArray)

      val paramsMap: Map[String, String] = jMap("transformer_params").asObject.get.toMap.flatMap {
        case (key, obj) =>
          key match {
            case "sublinear_tf" => obj.asBoolean.map(key -> _.toString)
            case "use_idf" => obj.asBoolean.map(key -> _.toString)
            case "norm" => obj.asString.map(key -> _)
            case _ => None
          }
      }

      GroupedFeaturesTfidfTransformerConfig(
        groups = groups,
        idf_diags = diags,
        n_features = numFeatures,
        transformer_params = paramsMap
      )
    }

    parseConfig(json.asObject.getOrElse(sys.error("object expected")).toMap)
  }

  def loadStandardScalerConfigFromJson(json: Json): ScalerConfig = {

    def parseConfig(jMap: Map[String, Json]) = ScalerConfig(
      withMean = jMap("with_mean").as[Boolean].fold(fail => sys.error(fail.getMessage()), v => v),
      withStd = jMap("with_std").as[Boolean].fold(fail => sys.error(fail.getMessage()), v => v),
      means = jMap("means").as[Array[Double]].fold(fail => sys.error(fail.getMessage()), v => v),
      scales = jMap("scales").as[Array[Double]].fold(fail => sys.error(fail.getMessage()), v => v)
    )

    parseConfig(json.asObject.getOrElse(sys.error("object expected")).toMap)
  }

  def loadPredictorWrapperConfigFromJson(json: Json): PredictorWrapperConfig = {

    def parseConfig(jMap: Map[String, Json]) = PredictorWrapperConfig(
      minFeaturesPerSample =
        jMap("min_features_per_sample").as[Int].fold(fail => sys.error(fail.getMessage()), v => v),
      maxFeaturesPerSample =
        jMap("max_features_per_sample").as[Int].fold(fail => sys.error(fail.getMessage()), v => v),
      predictLength =
        jMap("predict_length").as[Int].fold(fail => sys.error(fail.getMessage()), v => v)
    )

    parseConfig(json.asObject.getOrElse(sys.error("object expected")).toMap)
  }

  def loadPMMLEstimatorConfigFromJson(json: Json): PMMLEstimatorConfig = {

    def parseConfig(jMap: Map[String, Json]) = PMMLEstimatorConfig(
      featuresLength =
        jMap("input_length").as[Int].fold(fail => sys.error(fail.getMessage()), v => v),
      predictLength =
        jMap("predict_length").as[Int].fold(fail => sys.error(fail.getMessage()), v => v),
      pmmlDump = jMap("pmml_dump").as[String].fold(fail => sys.error(fail.getMessage()), v => v)
    )

    parseConfig(json.asObject.getOrElse(sys.error("object expected")).toMap)
  }

  def loadScoreEqualizerConfigFromJson(json: Json): ScoreEqualizerConfig = {
    def parseConfig(jMap: Map[String, Json]) = ScoreEqualizerConfig(
      minval = jMap("minval").as[Double].fold(decodeErr, v => v),
      maxval = jMap("maxval").as[Double].fold(decodeErr, v => v),
      noise = jMap("noise").as[Double].fold(decodeErr, v => v),
      eps = jMap("eps").as[Double].fold(decodeErr, v => v),
      coefficients =
        jMap("coefficients").as[Seq[Seq[Double]]].fold(decodeErr, v => v.map(_.toArray).toArray),
      intervals = jMap("intervals").as[Seq[Double]].fold(decodeErr, v => v.toArray)
    )

    parseConfig(json.asObject.getOrElse(sys.error("object expected")).toMap)
  }

  def loadLalTfidfScaledSgdcModelConfigFromJson(json: Json): LalTfidfScaledSgdcModelConfig = {
    // TODO: simplify, add some abstractions, pull out common code

    // load config components from json
    val jsonObject = json.asObject.getOrElse(sys.error("malformed model config json"))

    // preprocessors
    val imputerKey = "imputer_repr"
    val tfidfKey = "tfidf_repr"
    val scalerKey = "scaler_repr"
    // predictor (slicer have no config)
    val predictorKey = "predictor_wrapper_repr"
    // postprocessor
    val equalizerKey = "equalizer_repr"

    val imputerConfig: Array[Double] = {
      val imputerJson =
        jsonObject(imputerKey).getOrElse(sys.error(s"can't get '${imputerKey}' from json"))
      loadImputerConfigFromJson(imputerJson)
    }

    val tfidfConfig = {
      val tfidfJson =
        jsonObject(tfidfKey).getOrElse(sys.error(s"can't get '${tfidfKey}' from json"))
      loadGroupedFeaturesTfidfTransformerConfigFromJson(tfidfJson)
    }

    val scalerConfig = {
      val scalerJson =
        jsonObject(scalerKey).getOrElse(sys.error(s"can't get '${scalerKey}' from json"))
      loadStandardScalerConfigFromJson(scalerJson)
    }

    val (predictorWrapperConfig, predictorConfig) = {
      val predictorJson =
        jsonObject(predictorKey).getOrElse(sys.error(s"can't get '${predictorKey}' from json"))
      val wrapperConfig = loadPredictorWrapperConfigFromJson(predictorJson)

      val classifierConfig = {
        val key = "predictor_repr"
        val jsonObject = predictorJson.asObject.getOrElse(sys.error("object expected"))
        val classifierJson = jsonObject(key).getOrElse(sys.error(s"can't get '${key}' from json"))
        loadPMMLEstimatorConfigFromJson(classifierJson)
      }

      (wrapperConfig, classifierConfig)
    }

    val equalizerConfig = {
      val equalizerJson =
        jsonObject(equalizerKey).getOrElse(sys.error(s"can't get '${equalizerKey}' from json"))
      loadSBGroupedEqualizersConfigFromJson(equalizerJson)
    }

    LalTfidfScaledSgdcModelConfig(
      imputerConfig,
      tfidfConfig,
      scalerConfig,
      predictorWrapperConfig,
      predictorConfig,
      equalizerConfig
    )
  }

  def loadLalBinarizedMultinomialNbModelConfigFromJson(json: Json)
    : LalBinarizedMultinomialNbModelConfig = {
    // load config components from json
    val jsonObject = json.asObject.getOrElse(sys.error("malformed model config json"))

    // preprocessors
    val imputerKey = "imputer_repr"
    val binarizerKey = "binarizer_repr"
    // predictor (slicer have no config)
    val predictorKey = "predictor_wrapper_repr"
    // postprocessor
    val equalizerKey = "equalizer_repr"

    val imputerConfig: Array[Double] = {
      val imputerJson =
        jsonObject(imputerKey).getOrElse(sys.error(s"can't get '${imputerKey}' from json"))
      loadImputerConfigFromJson(imputerJson)
    }

    val binarizerConfig: BinarizerConfig = {
      val binarizerJson =
        jsonObject(binarizerKey).getOrElse(sys.error(s"can't get '${binarizerKey}' from json"))
      loadBinarizerConfigFromJson(binarizerJson)
    }

    val (predictorWrapperConfig, predictorConfig) = {
      val predictorJson =
        jsonObject(predictorKey).getOrElse(sys.error(s"can't get '${predictorKey}' from json"))
      val wrapperConfig = loadPredictorWrapperConfigFromJson(predictorJson)

      val classifierConfig: MultinomialNBConfig = {
        val key = "predictor_repr"
        val jsonObject = predictorJson.asObject.getOrElse(sys.error("object expected"))
        val classifierJson = jsonObject(key).getOrElse(sys.error(s"can't get '${key}' from json"))
        loadMultinomialNBConfigFromJson(classifierJson)
      }

      (wrapperConfig, classifierConfig)
    }

    val equalizerConfig = {
      val equalizerJson =
        jsonObject(equalizerKey).getOrElse(sys.error(s"can't get '${equalizerKey}' from json"))
      loadSBGroupedEqualizersConfigFromJson(equalizerJson)
    }

    LalBinarizedMultinomialNbModelConfig(
      imputerConfig,
      binarizerConfig,
      predictorWrapperConfig,
      predictorConfig,
      equalizerConfig
    )
  }

  def loadScoreAudiencePredictorFromJson(json: Json): Predictor = {
    import io.circe._

    // get values from json object
    def getFirstList(json: Json, key: String): List[Double] =
      json
        .findAllByKey(key)
        .head
        .as[List[Double]]
        .fold(e => sys.error(s"can't find array '${key}' in json: ${e.getMessage}"), lst => lst)

    val W: Array[Double] = getFirstList(json, "W").toArray
    val d_idf: Array[Double] = getFirstList(json, "d_idf").toArray

    val b: Double = json
      .findAllByKey("b")
      .head
      .as[Double]
      .fold(e => sys.error(s"can't find array 'b' in json: ${e.getMessage}"), x => x)

    Predictor(W, b, d_idf)
  }
}
