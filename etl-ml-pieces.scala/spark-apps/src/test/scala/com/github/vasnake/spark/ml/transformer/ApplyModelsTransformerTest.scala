/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.spark.ml.transformer

import com.github.vasnake.`etl-core`._
import com.github.vasnake.`ml-models`.complex._
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.json.JsonToolbox
import com.github.vasnake.json.read.ModelConfig
import com.github.vasnake.spark.test.SimpleLocalSpark
import com.github.vasnake.test.{ Conversions => CoreConversions }
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class ApplyModelsTransformerTest extends AnyFlatSpec with should.Matchers with SimpleLocalSpark {
  import ApplyModelsTransformerTest._
  import ApplyModelsTransformer._
  import CoreConversions.implicits._

  it should "parse parameters for one model" in {

    // transformer parameter: models list (one model)
    val strDescr = "a->hdfs:/path/to=42,b->foo,c->3.14"
    val mapDescr = Map("a" -> "hdfs:/path/to=42", "b" -> "foo", "c" -> "3.14")

    val transformer = new ApplyModelsTransformer() {
      // on load: generate dummy model
      override def loadModel(description: ModelDescription): ComplexMLModel =
        new DummyModel(description) {
          override def isOK: Boolean = true
        }
      // access to parsed config
      def getConfig: ApplyModelsTransformerConfig = config
    }.setModels(strDescr)

    // check that transformer parameters parsed successfully
    assert(transformer.initialize() === true)
    assert(transformer.getModels === strDescr)
    assert(transformer.getKeepColumns === "")

    // check parsed model description
    import scala.language.reflectiveCalls
    val cfg = transformer.getConfig
    assert(cfg.models.head.asInstanceOf[DummyModel].description === mapDescr)
    assert(cfg.keepColumns.isEmpty)
    assert(cfg.models.size === 1)
  }

  it should "parse parameters for batch of models" in {
    val map_descr_list = getDummyModelDescriptions(count = 3)
    val descriptions = map_descr_list.map(_._2).mkString(DescriptionTools.CFG_MODELS_SEPARATOR)

    val transformer = new ApplyModelsTransformer() {
      // on load: generate dummy model
      override def loadModel(description: ModelDescription): ComplexMLModel =
        new DummyModel(description) {
          override def isOK: Boolean = true
        }
      // access to config
      def getConfig: ApplyModelsTransformerConfig = config
    }
      .setModels(descriptions)
      .setKeepColumns("uid")

    // check parameters
    assert(transformer.initialize() === true)
    assert(transformer.getModels === descriptions)
    assert(transformer.getKeepColumns === "uid")

    // check descriptions parsing
    import scala.language.reflectiveCalls
    val cfg = transformer.getConfig
    assert(cfg.keepColumns === Seq("uid"))
    assert(cfg.models.size === 3)

    map_descr_list
      .map(_._1) // get descriptions as maps
      .zip(cfg.models) // pair with models
      .foreach(descr_model => // compare parsed with original
        assert(descr_model._2.asInstanceOf[DummyModel].description === descr_model._1)
      )
  }

  it should "SerDes parameters" in {
    if (isMsWindows) println("Windows platform, you need to fix Hadoop native libs first")
    else {
      val map_descr_list = getDummyModelDescriptions(count = 7)
      val descriptions = map_descr_list.map(_._2).mkString(DescriptionTools.CFG_MODELS_SEPARATOR)

      val transformer: ApplyModelsTransformer = new ApplyModelsTransformer()
        .setModels(descriptions)
        .setKeepColumns("uid")

      // serialize
      val path = "target/test_pipeline"
      val pipeline = new Pipeline().setStages(Array(transformer))
      pipeline.write.overwrite.save(path)

      // deserialize
      val ppl = Pipeline.read.load(path)
      assert(ppl.getStages.length === 1)
      val trans = ppl.getStages(0).asInstanceOf[ApplyModelsTransformer]
      assert(trans.getModels === descriptions)
    }
  }

  // don't needed but will be nice to have
  // it should "load spark.ml.pipeline serialized in python wrapper" in {
  //  ???
  // }

  it should "load from file and apply ScoringCustomLogRegSA model" in {
    import spark.implicits._

    val description = Map(
      "model_type" -> "ScoringCustomLogRegSA",
      "audience_name" -> "p_hmc_660",
      "model_path" -> FileToolbox.getResourcePath(this, "/p_hmc_660.json"),
      "features_path" -> "",
    )

    val transformer = new ApplyModelsTransformer()
      .setModels(DescriptionTools.packModelsDescriptions(Seq(description)))
      .setKeepColumns(DescriptionTools.packKeepColumns(Seq("uid")))

    val input: DataFrame = getSimpleInput.toDF

    assert(transformer.initialize() === true)
    val output = transformer.transform(input)

    output
      .select(
        ($"score" * 1000) cast "int",
        $"audience_name",
        $"uid",
      )
      .collect should contain theSameElementsAs Seq(
      (424, "p_hmc_660", "a")
    ).toDF.collect()
  }

  // TODO: add test case: load and apply pmml estimator with sparse inputFields

  it should "load from file and apply LalTfidfScaledSgdcPLSA model" in {
    import spark.implicits._

    val description = Map(
      "model_type" -> "LalTfidfScaledSgdcPLSA",
      "audience_name" -> "DM-6648_lal",
      "equalizer_selector" -> "VKID",
      "model_path" -> "/path/to/file/model.tar.gz",
      "model_repr_path" -> FileToolbox.getResourcePath(this, "/DM-6653/model_repr.json"),
      "features_path" -> FileToolbox.getResourcePath(this, "/DM-6653/grouped_features.json"),
    )

    import scala.language.reflectiveCalls
    val transformer = new ApplyModelsTransformer() { def getConfig = config }
      .setModels(DescriptionTools.packModelsDescriptions(Seq(description)))
      .setKeepColumns(DescriptionTools.packKeepColumns(Seq("uid")))

    val input: DataFrame =
      Seq(
        FeaturesDatasetRow(
          uid = "a0",
          topics_m = Map("0" -> 3.0f, "1" -> 4.0f, "2" -> 3.0f, "3" -> 42.0f),
          v1_groups_all = Map(),
          v2_groups_all = Map(),
          all_profiles = Array(),
        ),
        FeaturesDatasetRow(
          uid = "a1",
          topics_m = Map("0" -> 1.0f, "1" -> 0.0f, "2" -> 7.0f, "3" -> 10.0f),
          v1_groups_all = Map(),
          v2_groups_all = Map(),
          all_profiles = Array(),
        ),
      )
        .toDF
        .select($"uid", $"topics_m" as "0")

    assert(transformer.initialize() === true)

    lazy val checkLoadedConfig = {
      val config = transformer.getConfig.models.head.asInstanceOf[LalTfidfScaledSgdcModel].config

      assert(config.imputerConfig.toSeq === Seq(0.5f, 1.0f, 4.5f, 58.5f))

      assert(config.tfidfConfig.n_features === 4)
      assert(config.tfidfConfig.groups.map(_.toSeq).toSeq === Seq(Seq(0, 1, 2, 3)))
      assert(
        config.tfidfConfig.idf_diags.map(_.toSeq.map(x => (x * 1000).toInt)).toSeq === Seq(
          Seq(1510, 1510, 1000, 1000)
        )
      )
      assert(config.tfidfConfig.transformer_params("sublinear_tf") === "false")
      assert(config.tfidfConfig.transformer_params("use_idf") === "true")
      assert(config.tfidfConfig.transformer_params("norm") === "l1")

      assert(config.scalerConfig.scales.toSeq.map(x => (x * 1000).toInt) === Seq(23, 34, 14, 48))
      assert(!config.scalerConfig.withMean)
      assert(config.scalerConfig.withStd)
      assert(config.scalerConfig.means.isEmpty)

      assert(config.predictorWrapperConfig.predictLength === 2)
      assert(config.predictorWrapperConfig.minFeaturesPerSample === 1)
      assert(config.predictorWrapperConfig.maxFeaturesPerSample === 10000)

      assert(config.predictorConfig.predictLength === 2)
      assert(config.predictorConfig.featuresLength === 4)
      assert(config.predictorConfig.pmmlDump.contains("'estimator', SGDClassifier("))

      assert(config.equalizerConfig.groups.keys.toSeq.sorted === Seq("OKID", "VKID"))
      assert(
        config
          .equalizerConfig
          .groups("VKID")
          .asInstanceOf[ScoreEqualizerConfig]
          .maxval === 0.49397526451872636
      )
      assert(
        config.equalizerConfig.groups("OKID").asInstanceOf[ScoreEqualizerConfig].maxval === 0.5
      )

      true
    }
    assert(checkLoadedConfig === true)

    val output = transformer.transform(input)

    output
      .select(
        ($"score" * 1000) cast "int",
        ($"scores_raw".getItem(0) * 1000) cast "int",
        $"audience_name",
        $"uid",
      )
      .collect should contain theSameElementsAs Seq(
      (390, 472, "DM-6648_lal", "a0"),
      (0, 331, "DM-6648_lal", "a1"),
    ).toDF.collect()
  }

  it should "load from file and apply LalBinarizedMultinomialNbPLSA model" in {
    import spark.implicits._

    val description = Map(
      "model_type" -> "LalBinarizedMultinomialNbPLSA",
      "audience_name" -> "DM-6634_lal",
      "equalizer_selector" -> "VKID",
      "model_path" -> "/path/to/file/model.tar.gz",
      "model_repr_path" -> FileToolbox.getResourcePath(this, "/DM-6654/model_repr.json"),
      "features_path" -> FileToolbox.getResourcePath(this, "/DM-6654/grouped_features.json"),
    )

    import scala.language.reflectiveCalls
    val transformer = new ApplyModelsTransformer() { def getConfig = config }
      .setModels(DescriptionTools.packModelsDescriptions(Seq(description)))
      .setKeepColumns(DescriptionTools.packKeepColumns(Seq("uid")))

    val input: DataFrame =
      Seq(
        FeaturesDatasetRow(
          uid = "a0",
          topics_m = Map("0" -> 3.0f, "1" -> 4.0f, "2" -> 3.0f, "3" -> 42.0f),
          v1_groups_all = Map(),
          v2_groups_all = Map(),
          all_profiles = Array(),
        ),
        FeaturesDatasetRow(
          uid = "a1",
          topics_m = Map("0" -> 1.0f, "1" -> 0.0f, "2" -> 7.0f, "3" -> 10.0f),
          v1_groups_all = Map(),
          v2_groups_all = Map(),
          all_profiles = Array(),
        ),
      )
        .toDF
        .select($"uid", $"topics_m" as "one")

    assert(transformer.initialize() === true)

    lazy val checkLoadedConfig = {
      val config =
        transformer.getConfig.models.head.asInstanceOf[LalBinarizedMultinomialNbModel].config

      assert(
        config.imputerConfig.toSeq.toFloat === Seq(
          0.445695720357622, 0, 0.008073332670584632, -0.5132617216700879, 0.4789221006731517,
          -0.5557588304082495, -0.0008675537358615037, -0.4591643330356885, 0.5170261608432127,
          -0.46840322184867106, -0.008044922190276027, -0.56940394861928, 0.6199288129102266,
          -0.47064040134512786, 0.002275404946861126, -0.5358811001548798, 0.5029683241972684,
          -0.5155322519874553, 0.00044333591720735477, -0.4629614280940879,
        ).map(_.toFloat)
      )

      assert(config.binarizerConfig.threshold === 0.0f)

      assert(config.predictorWrapperConfig.predictLength === 2)
      assert(config.predictorWrapperConfig.minFeaturesPerSample === 1)
      assert(config.predictorWrapperConfig.maxFeaturesPerSample === 10000)

      assert(config.predictorConfig.predictLength === 2)
      assert(config.predictorConfig.featuresLength === 20)
      assert(
        config.predictorConfig.classLogPrior.toSeq.toFloat === Seq(
          -0.6931471805599453f,
          -0.6931471805599453f,
        )
      )
      assert(config.predictorConfig.featureLogProb.length === 2)

      assert(config.equalizerConfig.groups.keys.toSeq.sorted === Seq("OKID", "VKID"))

      assert(
        config
          .equalizerConfig
          .groups("VKID")
          .asInstanceOf[ScoreEqualizerConfig]
          .intervals
          .toSeq
          .map(_.toFloat) === Seq(0.0, 0.9976027000000001, 0.997602700001, 1.0).map(_.toFloat)
      )

      assert(
        config
          .equalizerConfig
          .groups("OKID")
          .asInstanceOf[ScoreEqualizerConfig]
          .intervals
          .toSeq
          .map(_.toFloat) === Seq(0.0, 0.9976027000000001, 0.997602700001, 1.0).map(_.toFloat)
      )

      true
    }
    assert(checkLoadedConfig === true)

    val output = transformer.transform(input)

    output
      .select(
        ($"score" * 1000) cast "int",
        ($"scores_raw".getItem(0) * 1000) cast "int",
        $"audience_name",
        $"uid",
      )
      .collect should contain theSameElementsAs Seq(
      (0, 803, "DM-6634_lal", "a0"),
      (0, 887, "DM-6634_lal", "a1"),
    ).toDF.collect()
  }

  it should "transform to correct schema" in {
    import spark.implicits._

    val input: DataFrame = getSimpleInput.toDF
    val predictor: Predictor = getSimplePredictor
    val transformer = getTransformer(Seq(ScoreAudienceModel("test_model_name", predictor)))
    assert(transformer.initialize() === true)

    val expected = StructType(
      Seq(
        StructField("score", DataTypes.DoubleType),
        StructField("scores_raw", DataTypes.createArrayType(DataTypes.DoubleType)),
        StructField("scores_trf", DataTypes.createArrayType(DataTypes.DoubleType)),
        StructField("audience_name", DataTypes.StringType),
        StructField("category", DataTypes.StringType),
      )
    )

    val outSchema = transformer.transformSchema(input.schema)
    val output = transformer.transform(input)

    outSchema.map(_.name) should contain theSameElementsAs expected.map(_.name)
    output.schema.map(_.name) should contain theSameElementsAs expected.map(_.name)
  }

  it should "keep given columns" in {
    import spark.implicits._

    val input: DataFrame = getSimpleInput.toDF
    val predictor: Predictor = getSimplePredictor
    val transformer = getTransformer(
      Seq(ScoreAudienceModel("test_model_name", predictor)),
      keepColumns = Seq("uid", "v1_groups_all"),
    )
    assert(transformer.initialize() === true)

    val expected = StructType(
      Seq(
        StructField("score", DataTypes.DoubleType),
        StructField("scores_raw", DataTypes.createArrayType(DataTypes.DoubleType)),
        StructField("scores_trf", DataTypes.createArrayType(DataTypes.DoubleType)),
        StructField("audience_name", DataTypes.StringType),
        StructField("category", DataTypes.StringType),
      )
    )

    val outSchema = transformer.transformSchema(input.schema)
    val output = transformer.transform(input)

    outSchema.map(_.name) should contain theSameElementsAs Seq("uid", "v1_groups_all") ++ expected
      .map(_.name)
    output.schema.map(_.name) should contain theSameElementsAs Seq(
      "uid",
      "v1_groups_all",
    ) ++ expected.map(_.name)
  }

  it should "apply set of models" in {
    import spark.implicits._

    val input: DataFrame = getSimpleInput.toDF
    val predictor: Predictor = getSimplePredictor
    val predictor2 =
      predictor.copy(W = predictor.W.map(_ * 2.0), b = 0.2, d_idf = predictor.d_idf.map(_ * 0.2))

    val transformer =
      getTransformer(Seq(ScoreAudienceModel("m1", predictor), ScoreAudienceModel("m2", predictor2)))

    assert(transformer.initialize() === true)
    val output = transformer.transform(input)

    val res = output
      .select(
        ($"score" * 1000) cast "int",
        $"audience_name",
      )
      .collect

    val expected = Seq(
      (474, "m1"),
      (450, "m2"),
    ).toDF.collect()

    res should contain theSameElementsAs expected
  }

  it should "load grouped features in given order" in {
    assert(
      loadGroupedFeaturesFromText("""{
        |"a": [1,2,3],
        |"b": [4,5,6],
        |"c": [7,8,9] }
        |""".stripMargin)
        .groups
        .map(_.name)
        .toSeq === Seq("a", "b", "c")
    )
    assert(
      loadGroupedFeaturesFromText("""{
          |"c": [7,8,9],
          |"a": [1,2,3],
          |"b": [4,5,6] }
          |""".stripMargin)
        .groups
        .map(_.name)
        .toSeq === Seq("c", "a", "b")
    )
    assert(
      loadGroupedFeaturesFromText("""{
          |"foo": [7,8,9],
          |"1": ["1","2","3"],
          |"0": [4,5,6] }
          |""".stripMargin)
        .groups
        .map(_.name)
        .toSeq === Seq("foo", "1", "0")
    )
    assert(
      loadGroupedFeaturesFromText("""{
          |"010": [7,8,9],
          |"101": ["1","2","3"],
          |"001": [4,5,6] }
          |""".stripMargin)
        .groups
        .map(_.name)
        .toSeq === Seq("010", "101", "001")
    )
  }

  it should "parse grouped features indices to right type (str or int)" in {
    val gf = loadGroupedFeaturesFromText(
      """ {
        |"a": [1, 2],
        |"b": ["foo","bar"],
        |"c": ["3", "4"] }
        |""".stripMargin.trim
    )

    assert(gf.groupIndices("a").get.toSeq === Seq("1", "2"))
    assert(gf.groupIndices("b").get.toSeq === Seq("foo", "bar"))
    assert(gf.groupIndices("c").get.toSeq === Seq("3", "4"))
    assert(gf.groups(0).isArray === true)
    assert(gf.groups(1).isArray === false)
    assert(gf.groups(2).isArray === false)
  }
}

object ApplyModelsTransformerTest {
  import ApplyModelsTransformer._

  def isMsWindows: Boolean = {
    // import org.apache.commons.lang3.JavaVersion
    import org.apache.commons.lang3.SystemUtils

    SystemUtils.IS_OS_WINDOWS
  }

  def getTransformer(grinderModels: Seq[ComplexMLModel], keepColumns: Seq[String] = Seq.empty)
    : ApplyModelsTransformer =
    new ApplyModelsTransformer() {
      override def buildConfig(): ApplyModelsTransformerConfig =
        ApplyModelsTransformerConfig(grinderModels, keepColumns)
    }

  private def loadGroupedFeaturesFromText(text: String): GroupedFeatures = {
    val json = JsonToolbox.parseJson(text)
    ModelConfig.loadGroupedFeaturesFromJson(json)
  }

  // simple model, only for testing
  case class DummyModel(description: ModelDescription) extends ComplexMLModel {
    override def isOK: Boolean = ???
    override def groupedFeatures: GroupedFeatures = ???
    override def apply(reatures: Array[Double]): Seq[Any] = ???
    override def toString: String = "DummyModel.description =" + description
      .map { case (k, v) => s""" "$k" -> "$v" """ }
      .mkString("\n")
  }

  // generate test data for transformer parameter
  def getDummyModelDescriptions(count: Int = 1): Seq[(ModelDescription, String)] = {
    val modelParams = Map(
      "model_type" -> "dummy",
      "audience_name" -> "p_hmc_660",
      "model_path" -> "/tmp/path/to/model_package",
      "features_path" -> "/tmp/path/to/grouped_features",
    )
    val res = (1 to count) map { num => modelParams.map(kv => (kv._1, s"${kv._2}-$num")) }

    res.map(paramsMap => (paramsMap, DescriptionTools.packModelsDescriptions(Seq(paramsMap))))
  }

  def getSimpleInput: Seq[FeaturesDatasetRow] = Seq(
    FeaturesDatasetRow(
      uid = "a",
      topics_m = Map("topic_107" -> 0.1f),
      v1_groups_all = Map(),
      v2_groups_all = Map(),
      all_profiles = Array(0.2f, 0.3f, 0.4f, 0.5f, 0.6f),
    )
  )

  def getSimplePredictor: Predictor = {
    // 200 topics + 4 main features from all_profiles
    val W = ((1 to 205) map { i => 1 / 1000.0 }).toArray
    val b = 0.1
    // 200 topics
    val d_idf = ((1 to 201) map { i => 1 / 2000.0 }).toArray

    Predictor(W, b, d_idf)
  }

  case class FeaturesDatasetRow(
    uid: String,
    topics_m: Map[String, Float],
    v1_groups_all: Map[String, Double],
    v2_groups_all: Map[String, Double],
    all_profiles: Array[Float],
  )

  case class AudienceDatasetRow(
    uid: String,
    score: Double,
    scores_raw: Array[Double],
    scores_trf: Array[Double],
    audience_name: String,
    category: String,
  )
}
