/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.spark.ml.estimator

import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.spark.test.ColumnValueParser.parseArrayDouble
import com.github.vasnake.spark.test._

import org.apache.spark.sql
import sql._
import sql.types._

import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class NEPriorClassProbaTest
    extends AnyFlatSpec
       with should.Matchers
       with LocalSpark
       with DataFrameHelpers {

  // invalid input, X: row.length != n_classes, row.sum == 0, row.exists(nan or null or negative), n_cols < 2, n_rows < 1
  // invalid parameters, prior: exists(v <= 0)
  // require: n_classes == n_cols == prior.length and n_classes > 1
  import NEPriorClassProbaTest._
  lazy val inputDF: DataFrame = createInputDF(spark).cache()
  val check5: (DataFrame, DataFrame) => Assertion = assertResult(5)

  it should "pass smoke test" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'A'")
    show(input, message = "input")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Array(3.0, 7.0))
      .setGroupColumns(Array.empty[String])
      .setSampleSize(1000)
      .setSampleRandomSeed(2021.04)
      .setCacheSettings("")

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 1)
    assertVectors(model.groupsConfig.head._2.toArray, Seq(1.39534884, 0.69767442), accuracy = 7)

    val output = model
      .setInputCol("probs")
      .setOutputCol("probs_aligned")
      .setGroupColumns(Array.empty[String])
      .transform(input)
      .cache()

    show(output, message = "output", force = true)
    check5(output, input)
  }

  it should "pass reference test B" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'B'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(5, 6))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test C" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'C'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 7, 11))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test D" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'D'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(1, 1))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test E" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'E'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(11, 11))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test F" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'F'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(11, 11))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test G" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'G'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(5, 3))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test H" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'H'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 5))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "pass reference test with refdata.json" in {
    // (xs: Array[Array[Double]], prior: Array[Double], aligner: Array[Double], probs: Array[Array[Double]])
    val referenceData = loadReferenceData("/DM-8183-ne_prior_class/DM-8183.refdata.json")

    val input: DataFrame = referenceData
      .toDataFrame(spark) // (uid, xs, probs)
      .withColumnRenamed("probs", "expected_probs_aligned")
      .withColumnRenamed("xs", "probs")
      .cache()
    assert(input.count() === 1000)

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(referenceData.prior)

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 1)
    assertVectors(model.groupsConfig.head._2.toArray, referenceData.aligner, accuracy = 7)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
    input.unpersist()
  }

  it should "filter out invalid input on fit stage" in {
    // invalid input, I, prior(3, 7, 11)
    // invalid if X: row.length != n_classes, row.sum == 0, row.exists(nan or null or negative), n_cols < 2, n_rows < 1
    // require: n_classes == n_cols == prior.length and n_classes > 1

    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'I'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 7, 11))
      .setGroupColumns(Seq("uid")) // one record => one group, invalid record => empty group => no fitted model

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 1) // only one good record
    assert(model.groupsConfig.head._1 === "g/h")

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "transform invalid data to null" in {
    // invalid input, part I, prior(3, 7, 11)
    // invalid if X: row.length != n_classes, row.sum == 0, row.exists(nan or null or negative), n_cols < 2, n_rows < 1
    // require: n_classes == n_cols == prior.length and n_classes > 1

    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'I'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 7, 11))

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 1)
    assert(model.groupsConfig.head._1 === "g")

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    show(output, "part I, output")
    check5(output, input)
  }

  it should "consider non-existent model as invalid data" in {
    // invalid input, I, prior(3, 7, 11)
    // invalid if X: row.length != n_classes, row.sum == 0, row.exists(nan or null or negative), n_cols < 2, n_rows < 1
    // require: n_classes == n_cols == prior.length and n_classes > 1

    val invalidInput = inputDF.selectExpr("uid", "probs").where("part = 'I'")
    val validInput =
      inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'J'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 7, 11))
      .setGroupColumns(Seq("uid"))

    // fit on invalid data, should produce 1 good model
    val model = estimator.fit(invalidInput)
    assert(model.groupsConfig.length === 1) // only one good record
    assert(model.groupsConfig.head._1 === "g/h")

    // transform valid data with unfitted models, except one
    val output = model
      .setOutputCol("probs_aligned")
      .transform(validInput)
      .cache()

    show(output, "part J, output")
    check5(output, validInput)
  }

  it should "transform to null with unfitted model" in {
    val invalidInput = inputDF.selectExpr("uid", "probs").where("part = 'I' and uid != 'h'") // no valid data
    val validInput = inputDF
      .selectExpr("uid", "probs", "expected_probs_aligned")
      .where("part = 'J' and uid != 'h'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 7, 11))

    val model = estimator.fit(invalidInput)
    assert(model.groupsConfig.length === 0) // no fitted models

    val output = model
      .setOutputCol("probs_aligned")
      .transform(validInput)
      .cache()

    check5(output, validInput)
  }

  it should "fit/apply different models with stratification" in {
    // fit/apply different models with stratification, (K, L), prior(3, 7, 11)

    val input = inputDF
      .selectExpr("uid", "part", "probs", "expected_probs_aligned")
      .where("part in ('K', 'L')")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setGroupColumns(Seq("part"))
      .setPriorValues(Seq(3, 7, 11))

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 2)

    val output = model
      .setOutputCol("probs_aligned")
      .transform(input)
      .cache()

    check5(output, input)
  }

  it should "affect train set with sampling" in {
    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'M'")
    show(input, "source")
    val _ = """
+---+---------------------+-------------------------------+
|uid|probs                |expected_probs_aligned         |
+---+---------------------+-------------------------------+
|a  |[0.0, 1.0, 0.0]      |[0.0, 1.0, 0.0]                |
|b  |[33.0, 2.0, 43.0]    |[0.1036757, 0.004712, 0.89161] |
|c  |[111.0, 3.0, 333.0]  |[0.04803, 9.73E-4, 0.950996]   |
|d  |[3.0, 4.0, 2.0]      |[0.156249, 0.15625, 0.6875]    |
|e  |[33.0, 44.0, 5.0]    |[0.333333, 0.333333, 0.333333] |
|f  |[111.0, 222.0, 333.0]|[0.044843, 0.06726457, 0.88789]|
|g  |[0.0, 6.0, 0.0]      |[0.0, 1.0, 0.0]                |
|h  |[33.3, 44.4, 43.21]  |[0.094659, 0.094659, 0.81068]  |
+---+---------------------+-------------------------------+

    """

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Seq(3, 7, 11))
      .setSampleSize(1)
      .setSampleRandomSeed(0) // java rnd under the hood, we need deterministic rnd for tests
      .setCacheSettings("cache")

    val model = estimator.fit(input)

    assert(model.groupsConfig.length === 1)

    val output = cache(
      model
        .setOutputCol("probs_aligned")
        .transform(input)
    )

    check5(output, input)
  }

  it should "check parameters on validation stage" in {
    // testOnly *model.NEPriorClass* -- -z "check parameters"

    // invalid parameters => exception on validation stage
    // invalid if prior: exists(v <= 0)
    // require: n_classes == n_cols == prior.length and n_classes > 1
    // require: input, group columns exists; output col not exists or empty

    val input = inputDF.selectExpr("uid", "probs", "expected_probs_aligned").where("part = 'A'")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("probs")
      .setPriorValues(Array(3.0, 7.0))
      .setGroupColumns(Array.empty[String])
      .setSampleSize(1000)
      .setSampleRandomSeed(2021.04)

    val model = estimator.fit(input)

    model
      .setInputCol("probs")
      .setOutputCol("probs_aligned")
      .setGroupColumns(Array.empty[String])
      .transform(input)
      .show()

    // input col not exists

    val actual = intercept[sql.AnalysisException] {
      model.setInputCol("_probs").transform(input)
        .show()
    }
    val expected = """name `_probs` cannot be resolved"""
    assert(actual.getMessage contains expected)

    assert(intercept[sql.AnalysisException] {
      model
        .setInputCol("probs")
        .setGroupColumns(Seq("_uid"))
        .transform(input)
        .show()
    }.getMessage.contains("""name `_uid` cannot be resolved"""))

    assert(intercept[IllegalArgumentException] {
      estimator.setInputCol("_probs")
        .fit(input)
    }.getMessage.contains("""_probs does not exist"""))

    assert(intercept[IllegalArgumentException] {
      estimator
        .setInputCol("probs")
        .setGroupColumns(Seq("_uid"))
        .fit(input)
    }.getMessage.contains("""_uid does not exist"""))

    // output col exists
    assert(intercept[IllegalArgumentException] {
      model
        .setInputCol("probs")
        .setGroupColumns(Seq.empty)
        .setOutputCol("expected_probs_aligned")
        .transform(input)
        .show()
    }.getMessage.contains("""column `expected_probs_aligned` already exists"""))

    // output col is empty
    assert(intercept[IllegalArgumentException] {
      model
        .setOutputCol("")
        .transform(input)
        .show()
    }.getMessage.contains("""Output column name can't be empty"""))

    // prior contains x <= 0
    assert(intercept[IllegalArgumentException] {
      estimator
        .setInputCol("probs")
        .setGroupColumns(Seq.empty)
        .setPriorValues(Seq(1.0, 0.0))
        .fit(input)
    }.getMessage.contains("""min value must be > 0"""))

    // n_classes < 2
    assert(intercept[IllegalArgumentException] {
      estimator
        .setPriorValues(Seq(1))
        .fit(input)
    }.getMessage.contains("""Prior size must be > 1 and min value must be > 0"""))

  }

  it should "filter DF using sql array functions" in {
    val input = inputDF.selectExpr("uid", "probs").where("part = 'SAF'")

    // invalid records

    // array is null
    check(expr = "probs is null", expected = Seq("a"))

    // array size is wrong
    check(expr = "size(probs) != 3", expected = Seq("a", "b1", "b2", "b3"))

    // array elements < 0
    check(expr = "array_min(probs) < 0", expected = Seq("g"))
    check(expr = "exists(probs, x -> x < 0)", expected = Seq("g"))

    // array.exists(x is null)
    check(expr = "exists(probs, x -> x is null)", expected = Seq("c", "e", "f"))
    check(expr = "exists(probs, x -> isnull(x))", expected = Seq("c", "e", "f"))

    // array.exists(x is nan)
    check(expr = "exists(probs, x -> isnan(x))", expected = Seq("d", "e", "f"))

    // array.sum = 0
    show(
      input.selectExpr(
        "uid",
        "aggregate(probs, cast(0 as double), (acc, x) -> acc + cast(x as double)) as sum"
      ),
      "aggregate"
    )
    check(
      expr = "aggregate(probs, cast(0 as double), (acc, x) -> acc + cast(x as double)) = 0",
      expected = "b1,g,h".split(',')
    )

    // one valid record
    check(
      expr = "probs is not null and " +
        "size(probs) = 3 and " +
        "not exists(probs, x -> isnull(x) or isnan(x) or x < 0) and " +
        "exists(probs, x -> x > 0)",
      expected = Seq("i")
    )

    import spark.implicits._
    def check(expr: String, expected: Iterable[String]): Assertion =
      input
        .where(expr)
        .selectExpr("uid")
        .as[String]
        .collect()
        .toList should contain theSameElementsAs expected.toList

  }

  it should "not fail if data col name is x" in {
    val input = inputDF.selectExpr("uid", "probs as x").where("part = 'C'")
    show(input, message = "input")

    val estimator = new NEPriorClassProbaEstimator()
      .setInputCol("x")
      .setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(input)

    val output = model
      .setOutputCol("y")
      .transform(input)
      .cache()

    show(output, message = "output", force = true)
  }

  private def assertVectors(
    actual: Iterable[Double],
    expected: Iterable[Double],
    accuracy: Int = 7
  ): Assertion = {
    val a = actual.toList
    val e = expected.toList
    assert(a.length === e.length)
    assert(a.map(s"%1.${accuracy}f".format(_)) === e.map(s"%1.${accuracy}f".format(_)))
  }

  private def assertResult(accuracy: Int)(actual: DataFrame, expected: DataFrame): Assertion = {
    assert(expected.count() > 0)

    compareSchemas(
      actual.schema,
      expected
        .schema
        .add(StructField("probs_aligned", DataTypes.createArrayType(DataTypes.DoubleType)))
    )

    assert(actual.count === expected.count)

    compareArrayDoubleColumns(
      actual,
      expected,
      "probs_aligned",
      "expected_probs_aligned",
      accuracy = accuracy
    )

    compareDataframes(
      actual.drop("expected_probs_aligned"),
      expected.withColumnRenamed("expected_probs_aligned", "probs_aligned"),
      accuracy = accuracy,
      unpersist = true
    )
  }
}

object NEPriorClassProbaTest {
  def createInputDF(spark: SparkSession): DataFrame =
    // uid, part, probs, expected_probs_aligned
    spark.createDataFrame(
      Seq(
        // spark.sql.array_func
        InputRow("a", "SAF", "null", ""),
        InputRow("b1", "SAF", "", ""),
        InputRow("b2", "SAF", "1, 2", ""),
        InputRow("b3", "SAF", "1, 2, 3, 4", ""),
        InputRow("c", "SAF", "1, null, 2", ""),
        InputRow("d", "SAF", "3, nan, 4", ""),
        InputRow("e", "SAF", "5, nan, null", ""),
        InputRow("f", "SAF", "null, nan, null", ""),
        InputRow("g", "SAF", "-6, 2, 4", ""),
        InputRow("h", "SAF", "0, 0, 0", ""),
        InputRow("i", "SAF", "0.1, 9, 0", ""),
        // smoke test, prior(3, 7)
        InputRow("a", "A", "1.1, 2.2", "0.5, 0.5"),
        // ref 1-B, prior(5, 6)
        InputRow("a", "B", "1.1, 2.2", "0.272727, 0.727273"),
        InputRow("b", "B", "44.4, 33.3", "0.5, 0.5"),
        // ref 2-C, prior(3, 7, 11)
        InputRow("a", "C", "1.1, 2.2, 3.3", "0.25000, 0.375, 0.375"),
        InputRow("b", "C", "33.3, 44.4, 43.21", "0.37753, 0.37753, 0.244941"),
        InputRow("c", "C", "111.1, 222.2, 333.3", "0.25, 0.375, 0.375"),
        // ref 3-D, prior(1, 1)
        InputRow("a", "D", "0.5, 0.5", "0.524083, 0.475917"),
        InputRow("b", "D", "0.5, 0.5", "0.524083, 0.475917"),
        InputRow("c", "D", "0.5, 0.5", "0.524083, 0.475917"),
        // ref 4-E, prior(11, 11)
        InputRow("a", "E", "0.51, 0.49", "0.53405, 0.46595"),
        InputRow("b", "E", "0.51, 0.49", "0.53405, 0.46595"),
        InputRow("c", "E", "0.51, 0.49", "0.53405, 0.46595"),
        // ref 5-F, prior(11, 11)
        InputRow("a", "F", "0.51, 0.52", "0.471077, 0.528923"),
        InputRow("b", "F", "0.51, 0.52", "0.471077, 0.528923"),
        InputRow("c", "F", "0.51, 0.52", "0.471077, 0.528923"),
        // ref 6-G, prior(5, 3)
        InputRow("a", "G", "0.51, 0.52", "0.5, 0.5"),
        InputRow("b", "G", "0.52, 0.51", "0.509708, 0.490292"),
        InputRow("c", "G", "0.51, 0.52", "0.5, 0.5"),
        // ref 7-H, prior(3, 5)
        InputRow("a", "H", "0, 1", "0, 1"),
        InputRow("b", "H", "11, 2", "0.567440, 0.432560"),
        InputRow("c", "H", "2, 3", "0.137193, 0.862807"),
        InputRow("d", "H", "33, 4", "0.663041, 0.336959"),
        // invalid input, I, prior(3, 7, 11)
        // invalid if X: row.length != n_classes, row.sum == 0, row.exists(nan or null or negative), n_cols < 2, n_rows < 1
        // require: n_classes == n_cols == prior.length and n_classes > 1
        InputRow("a", "I", "0, 0, 0", "null"),
        InputRow("b", "I", "33, nan, 43", "null"),
        InputRow("c", "I", "111, null, 333", "null"),
        InputRow("d", "I", "3, -1, 2", "null"),
        InputRow("e", "I", "33, 44", "null"),
        InputRow("f", "I", "111, 222, 333, 444", "null"),
        InputRow("g", "I", "0, -0, 0", "null"),
        InputRow("h", "I", "33.3, 44.4, 43.21", "0.33333, 0.33333, 0.33333"), // one good record
        // valid input, complementary to `I`, prior(3, 7, 11)
        InputRow("a", "J", "0, 1, 0", "null"),
        InputRow("b", "J", "33, 2, 43", "null"),
        InputRow("c", "J", "111, 3, 333", "null"),
        InputRow("d", "J", "3, 4, 2", "null"),
        InputRow("e", "J", "33, 44, 5", "null"),
        InputRow("f", "J", "111, 222, 333", "null"),
        InputRow("g", "J", "0, 6, 0", "null"),
        InputRow("h", "J", "33.3, 44.4, 43.21", "0.33333, 0.33333, 0.33333"),
        // fit/apply different models with stratification, (K, L), prior(3, 7, 11)
        InputRow("a", "K", "33.3, 44.4, 43.21", "0.33333, 0.33333, 0.33333"), // same input, different output:
        InputRow("a", "L", "33.3, 44.4, 43.21", "0.37753, 0.37753, 0.244941"), // this group produces different model
        InputRow("b", "L", "1.1, 2.2, 3.3", "0.25000, 0.375, 0.375"),
        InputRow("c", "L", "111.1, 222.2, 333.3", "0.25, 0.375, 0.375"),
        // sampling affects train set, M, prior(3, 7, 11)
        InputRow("a", "M", "0, 1, 0", "0.00000,1.00000,0.00000"),
        InputRow("b", "M", "33, 2, 43", "0.33333,0.01515,0.65152"),
        InputRow("c", "M", "111, 3, 333", "0.18115,0.00367,0.81518"),
        InputRow("d", "M", "3, 4, 2", "0.33333,0.33333,0.33333"),
        InputRow("e", "M", "33, 44, 5", "0.44898,0.44898,0.10204"),
        InputRow("f", "M", "111, 222, 333", "0.14286,0.21429,0.64286"),
        InputRow("g", "M", "0, 6, 0", "0.00000,1.00000,0.00000"),
        InputRow("h", "M", "33.3, 44.4, 43.21", "0.25340,0.25340,0.49321")
      )
    )
    // uid, part, probs, expected_probs_aligned

  case class InputRow(
    uid: String,
    part: String,
    probs: Option[Array[Option[Double]]],
    expected_probs_aligned: Option[Array[Option[Double]]]
  )
  object InputRow {
    def apply(
      uid: String,
      part: String,
      probs: String,
      expected_probs_aligned: String
    ): InputRow =
      InputRow(
        uid,
        part,
        probs = parseArrayDouble(probs),
        expected_probs_aligned = parseArrayDouble(expected_probs_aligned)
      )
  }

  private def loadReferenceData(resourceName: String): ReferenceData = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats: DefaultFormats = DefaultFormats

    val filePath = FileToolbox.getResourcePath(this, resourceName)
    val text: String = FileToolbox.loadTextFile(filePath)
    assert(!(text.contains("NaN") || text.contains("nan"))) // text.replaceAllLiterally("NaN", "-0.0")

    parse(text).extract[ReferenceData]
  }

  case class ReferenceData(
    xs: Array[Array[Double]],
    prior: Array[Double],
    probs: Array[Array[Double]],
    aligner: Array[Double]
  ) {
    require(
      xs.length == probs.length && xs.head.length == prior.length,
      "In/out data shapes must be equal. [1]"
    )
    require(
      xs.indices.forall(i => xs(i).length == probs(i).length),
      "In/out data shapes must be equal. [2]"
    )

    def toDataFrame(
      implicit
      spark: SparkSession
    ): DataFrame =
      spark.createDataFrame(
        xs.indices
          .map(i =>
            ReferenceRow(
              uid = s"uid_${i}",
              xs = xs(i),
              probs = probs(i)
            )
          )
      )
  }

  case class ReferenceRow(
    uid: String,
    xs: Array[Double],
    probs: Array[Double]
  )
}
