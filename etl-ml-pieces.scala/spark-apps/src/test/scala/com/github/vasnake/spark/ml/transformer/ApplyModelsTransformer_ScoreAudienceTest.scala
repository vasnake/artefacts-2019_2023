/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.spark.ml.transformer

import com.github.vasnake.`ml-models`.complex._
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.spark.ml.transformer.{ ApplyModelsTransformer => amt }
import com.github.vasnake.spark.test.SimpleLocalSpark
import org.apache.spark.sql._
import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class ApplyModelsTransformer_ScoreAudienceTest {}

class ScoreAudienceTest extends AnyFlatSpec with should.Matchers with SimpleLocalSpark {
  import ScoreAudienceTest._
  import ApplyModelsTransformerTest._

  it should "load and apply model from file" in {
    import spark.implicits._

    val description = Map(
      "model_type" -> "ScoringCustomLogRegSA",
      "audience_name" -> "p_hmc_660",
      "model_path" -> FileToolbox.getResourcePath(this, "/p_hmc_660.json"),
      "features_path" -> "",
    )

    val transformer = buildTransformer(spark)
      .setModels(amt.DescriptionTools.packModelsDescriptions(Seq(description)))
      .setKeepColumns(amt.DescriptionTools.packKeepColumns(Seq("uid")))

    assert(transformer.initialize() === true)

    val output = transformer.transform(getSimpleInput.toDF)

    val res = output
      .select(
        ($"score" * 1000) cast "int",
        $"audience_name",
        $"uid",
      )
      .collect

    val expected = Seq(
      (424, "p_hmc_660", "a")
    ).toDF.collect()

    res should contain theSameElementsAs expected
  }

  it should "score audience" in {
    import spark.implicits._
    val transformer = getTransformer(Seq(ScoreAudienceModel("test_model_name", getSimplePredictor)))
    assert(transformer.initialize() === true)
    val output = transformer.transform(getSimpleInput.toDF)

    val res = output
      .select(
        ($"score" * 1000) cast "int",
        $"audience_name",
      )
      .collect

    val expected = Seq(
      (474, "test_model_name")
    ).toDF.collect()

    res should contain theSameElementsAs expected
  }

  it should "use default values in features" in {
    import spark.implicits._

    val input: DataFrame = Seq(InputDatasetRow("a", None, None)).toDF

    val predictor: Predictor = getSimplePredictor
    val transformer = getTransformer(Seq(ScoreAudienceModel("test_model_name", predictor)))
    assert(transformer.initialize() === true)

    val output = transformer.transform(input)
    val res = output.select(($"score" * 1000) cast "int").collect
    val expected = Seq(474).toDF.collect()

    res should contain theSameElementsAs expected
  }

  it should "compute reference score value" in {
    import spark.implicits._

    val input: DataFrame = Seq(
      FeaturesDatasetRow(
        uid = "a",
        topics_m = Map(
          ("topic_3", 0.3068109024215272f),
          ("topic_4", 0.9344817462838555f),
          ("topic_5", 0.7174355335110215f),
          ("topic_6", 0.8060563109627388f),
          ("topic_10", 0.7705491752698755f),
        ),
        v1_groups_all = Map(),
        v2_groups_all = Map(),
        all_profiles = Array(
          0.9164074522494015f,
          0.5884483607798774f,
          0.12720521573031884f,
          0.12720521573031884f,
        ),
      )
    ).toDF

    val description = Map(
      "model_type" -> "ScoringCustomLogRegSA",
      "audience_name" -> "p_hmc_660",
      "model_path" -> FileToolbox.getResourcePath(this, "/p_hmc_660.json"),
      "features_path" -> "",
    )

    val transformer = buildTransformer(spark)
      .setModels(amt.DescriptionTools.packModelsDescriptions(Seq(description)))
    assert(transformer.initialize() === true)

    val output = transformer.transform(input)
    val res = output
      .select(
        ($"score" * 1000) cast "int",
        $"audience_name",
      )
      .collect

    val expected = Seq(
      (689, "p_hmc_660")
    ).toDF.collect

    res should contain theSameElementsAs expected
  }

  it should "fail before scores computation, if no features field in dataframe" in {
    // TODO: add test case: transformer fail if DataFrame don't have column with features
    val expected = 392
    val magn = 1000.0

    // checkScore(
    //  input = Seq("42").toDF("uid"),
    //  expected,
    //  magn
    // )
  }

  it should "compute score for all_profiles is null and topics_motor200 is null" in {
    val expected = 392
    val magn = 1000.0

    checkScore(
      input = Seq(InputDatasetRow("a", None, None)),
      expected,
      magn,
    )
  }

  it should "compute score for all_profiles is null and topics_motor200 is not null" in {
    // all_profiles is null
    // topics is filled with values (0.11 .. 0.99)
    val topics =
      buildSyntheticTopics(step = (0.99 - TOPIC_THRESHOLD) / 200.0, minval = TOPIC_THRESHOLD + 0.01)
    val expected = 9977
    val magn = 10000.0

    checkScore(
      input = Seq(InputDatasetRow("a", Some(topics), None)),
      expected,
      magn,
    )
  }

  it should "compute score for all_profiles is not null and topics_motor200 is null" in {
    // all_profiles is filled
    // topics is null
    // (age, sex, joined, num_vids, has_stop_phone)
    val allProfs: Array[Float] = Array(0.55f, 0.66f, 0.77f, 0.44f, 0.33f)
    val expected = 3792
    val magn = 10000.0

    checkScore(
      input = Seq(InputDatasetRow("a", None, Some(allProfs))),
      expected,
      magn,
    )

    // only first three values are needed
    checkScore(
      input = Seq(InputDatasetRow("a", None, Some(allProfs.take(3)))),
      expected,
      magn,
    )
  }

  it should "compute score for all_profiles is not null and topics_motor200 is not null" in {
    val topics =
      buildSyntheticTopics(step = (0.99 - TOPIC_THRESHOLD) / 200.0, minval = TOPIC_THRESHOLD + 0.01)

    val allProfs: Array[Float] = Array(0.55f, 0.66f, 0.77f)
    val expected = 9976
    val magn = 10000.0

    checkScore(
      input = Seq(InputDatasetRow("a", Some(topics), Some(allProfs))),
      expected,
      magn,
    )
  }

  it should "compute score for all_profiles = 0 and topics_motor200  = 0" in {
    // all_profiles set to 0
    // topics set to 0
    val topics = buildSyntheticTopics(step = 0.0, minval = 0.0)

    val allProfs: Array[Float] = Array(0.0f, 0.0f, 0.0f)
    val expected = 3965
    val magn = 10000.0

    checkScore(
      input = Seq(InputDatasetRow("a", Some(topics), Some(allProfs))),
      expected,
      magn,
    )
  }

  it should "compute score for all_profiles = 1 and topics_motor200  = 1" in {
    // all_profiles set to 1
    // topics set to 1
    val topics = buildSyntheticTopics(step = 0.0, minval = 1.0)

    val allProfs: Array[Float] = Array(1.0f, 1.0f, 1.0f)
    val expected = 2770
    val magn = 1e11

    checkScore(
      input = Seq(InputDatasetRow("a", Some(topics), Some(allProfs))),
      expected,
      magn,
    )
  }

  it should "compute score for all_profiles = 0.33 and topics_motor200  = 0.11" in {
    // all_profiles set to 0.33
    // topics set to 0.11
    val topics = buildSyntheticTopics(step = 0.0, minval = 0.11)

    val allProfs: Array[Float] = Array(0.33f, 0.33f, 0.33f)
    val expected = 9999
    val magn = 1e4

    checkScore(
      input = Seq(InputDatasetRow("a", Some(topics), Some(allProfs))),
      expected,
      magn,
    )
  }

  it should "compute score for arbitrary all_profiles and gradual topics_motor200" in {
    // all_profiles = (0.3, 0.2, 0.7)
    // topics = (0.11 .. 0.99)
    val topics = buildSyntheticTopics(step = (0.99 - 0.1) / 200.0, minval = 0.1)

    val allProfs: Array[Float] = Array(0.7f, 0.3f, 0.2f)
    val expected = 9989
    val magn = 1e4

    checkScore(
      input = Seq(InputDatasetRow("a", Some(topics), Some(allProfs))),
      expected,
      magn,
    )
  }

  it should "compute score for arbitrary all_profiles and topics_motor200 subset" in {
    // all_profiles = (0.6, 0.2, 0.3)
    // topics = topics on odd idx set to value from range (0.11 .. 0.99)
    val topics: Map[String, Float] = buildSyntheticTopics(
      step = (0.99 - 0.1) / 200.0,
      minval = 0.1,
    )

    val allProfs: Array[Float] = Array(0.6f, 0.2f, 0.1f)
    val expected = 9901
    val magn = 1e4

    // odd topics
    checkScore(
      input = Seq(
        InputDatasetRow(
          "a",
          Some(topics.filter {
            case (k, v) =>
              val num = k.split("_").apply(1).toInt
              num % 2 != 0
          }),
          Some(allProfs),
        )
      ),
      expected,
      magn,
    )

    // even topics
    checkScore(
      input = Seq(
        InputDatasetRow(
          "a",
          Some(topics.filter {
            case (k, v) =>
              val num = k.split("_").apply(1).toInt
              num % 2 == 0
          }),
          Some(allProfs),
        )
      ),
      expected = 9919,
      magn,
    )
  }

  def checkScore(
    input: Seq[InputDatasetRow],
    expected: Int,
    magn: Double,
  )(implicit
    spark: SparkSession
  ): Assertion = {
    import spark.implicits._
    val inputDF: DataFrame = input.toDF
    checkScore(inputDF, expected, magn)
  }

  def checkScore(
    input: DataFrame,
    expected: Int,
    magn: Double,
  )(implicit
    spark: SparkSession
  ): Assertion = {
    import spark.implicits._
    val transformer = getTransformer(
      Seq(ScoreAudienceModel("synthetic_predictor", buildSyntheticPredictor))
    )
    assert(transformer.initialize() === true)
    val output = transformer.transform(input)
    val res = output.select(($"score" * magn) cast "int").collect
    res should contain theSameElementsAs Seq(expected).toDF.collect
  }
}

object ScoreAudienceTest {
  private def buildTransformer(spark: SparkSession) =
    new ApplyModelsTransformer()

  case class InputDatasetRow(
    uid: String,
    topics_m: Option[Map[String, Float]],
    all_profiles: Option[Array[Float]],
  )

  val TOPIC_THRESHOLD: Double = 0.1

  def buildSyntheticTopics(step: Double, minval: Double): Map[String, Float] = {
    val topicNames = (1 to 200) map { num => s"topic_$num" }
    val topicValues = Range.inclusive(200, 1, -1) map { num => (num * step + minval).toFloat }
    topicNames.zip(topicValues).toMap
  }

  def buildSyntheticPredictor: Predictor = {
    // 200 topics + 4 main features from all_profiles
    val W: Array[Double] = ((1 to 205) map { num => num / 100.0 }).toArray
    val b = 0.42
    // 200 topics
    val d_idf = ((1 to 201) map { num => num / 200.0 }).toArray

    Predictor(W, b, d_idf)
  }
}
