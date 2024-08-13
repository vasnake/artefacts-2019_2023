/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package com.github.vasnake.spark.ml.estimator

import org.scalatest._
import flatspec._
import matchers._
//import org.scalactic.Equality

//import com.github.vasnake.test.{Conversions => CoreConversions}
//import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
//import com.github.vasnake.spark.test.ColumnValueParser.parseArrayDouble
//import com.github.vasnake.spark.ml.estimator
//import com.github.vasnake.core.text.StringToolbox
//import com.github.vasnake.`ml-models`.complex.ScoreEqualizerConfig
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.spark.test._

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField}

class ScoreQuantileThresholdTest extends AnyFlatSpec with should.Matchers with LocalSpark with DataFrameHelpers {

  import ScoreQuantileThresholdTest._

  lazy val inputDF: DataFrame = createInputDF(spark).cache()
  val check5: (DataFrame, DataFrame) => Assertion = assertResult(5)
  val check3: (DataFrame, DataFrame) => Assertion = assertResult(3)

  it should "pass smoke test" in {
    val input = inputDF.where("part = 'C'")
    show(input, message = "input")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0, 3.0, 4.0))
      .setLabels(Array("foo", "bar", "baz", "qux"))
      .setGroupColumns(Seq.empty)
      .setSampleSize(1000)
      .setSampleRandomSeed(3)

    val model = estimator.fit(input)

    val output = model.setOutputCol("class_index").setRankCol("rank")
      .transform(input)
      .cache()

    show(output, message = "output", force = true)
    check3(output, input)
  }

  it should "pass reference test" in {
    // no null items, only nan or valid number
    val referenceData = loadReferenceData("/DM-8181-quantile-threshold/refdata.json")

    val input: DataFrame = referenceData.toDataFrame(spark)
      .withColumnRenamed("class_index", "expected_index")
      .withColumnRenamed("rank", "expected_rank")
      .cache()

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("x")
      .setPriorValues(referenceData.prior)
      .setGroupColumns(Seq.empty)
      .setSampleSize(1000000)

    val model = estimator.fit(input)

    val output = {
      model
        .setInputCol("x")
        .setOutputCol("class_index")
        .setRankCol("rank")
        .setGroupColumns(Seq.empty)
    }
      .transform(input)
      .cache()

    check3(output, input)
  }

  it should "transform: null => (null, null), nan => (null, null), part N" in {
    val df = inputDF.where("part = 'N'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train")
      .setPriorValues(Array(1.0, 2.0))

    val model = estimator.fit(df)

    val res = model.setInputCol("score").setOutputCol("class_index").setRankCol("rank")
      .transform(df)
      .cache()

    check3(res, df)
  }

  it should "fit with train vector.size=1, part M" in {
    //  fit: should WARN if learning vector is shorter than num of classes; //  learn vector size < n_classes;
    // warn("Fitting quantiles to scores data of length less than classes amount")
    val df = inputDF.where("part = 'M'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train")
      .setPriorValues(Array(1.0, 2.0))

    val model = estimator.fit(df)

    val res = model.setInputCol("score").setOutputCol("class_index").setRankCol("rank")
      .transform(df)
      .cache()

    check3(res, df)
  }

  it should "not fail if no fitted groups, part L" in {
    //  fit, no models => failed fit
    val df = spark.createDataFrame(Seq(
      ("a", Some(Double.NaN), Some(0),  Some(Double.NaN)),
      ("b", None,             None,     None),
      ("c", Some(Double.NaN), Some(0),  Some(Double.NaN))
    )).toDF("uid", "score", "expected_index", "expected_rank")

    val model = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0))
      .setGroupColumns(Seq("uid"))
      .fit(df)

    assert(model.groupsConfig.isEmpty)
  }

  it should "not fail if at least one group fitted; transform unknown groups to null, part K" in {
    // fit, no valid data => no model;
    // fail if no valid values in xs (null, nan: invalid); // learning vector size = 0; //  learn data contains null and nan;
    // transform, unknown group => null;
    val df = inputDF.where("part = 'K'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train")
      .setPriorValues(Array(1.0, 2.0))
      .setGroupColumns(Seq("uid_type"))

    val model = estimator.fit(df)
    assert(model.groupsConfig.length === 1)

    val res = model.setInputCol("score").setOutputCol("class_index").setRankCol("rank")
      .transform(df)
      .cache()

    check3(res, df)
  }

  it should "fit-transform no-groups dataset, part J" in {
    // testOnly *QuantileThreshold* -- -z "no-groups"
    val input = inputDF.where("part = 'J'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0, 3.0, 4.0))
      .setGroupColumns(Seq.empty)

    val model = estimator.fit(input)

    val output = model.setOutputCol("class_index").setRankCol("rank")
      .transform(input)
      .cache()

    check3(output, input)
  }

  it should "fit-transform one group dataset, part I" in {
    // testOnly *QuantileThreshold* -- -z "one group"
    val input = inputDF.where("part = 'I'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0, 3.0))
      .setGroupColumns(Seq("uid"))

    val model = estimator.fit(input)

    val output = model.setOutputCol("class_index").setRankCol("rank")
      .transform(input)
      .cache()

    check3(output, input)
  }

  it should "fit-transform n groups dataset, part H" in {
    // stratification, should learn and apply 1..n models for 1..n groups defined by 0..n columns;
    // testOnly *QuantileThreshold* -- -z "n groups"
    val input = inputDF.where("part = 'H'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0, 3.0))
      .setGroupColumns(Seq("part", "uid_type", "uid"))

    val model = estimator.fit(input)

    val output = model.setOutputCol("class_index").setRankCol("rank")
      .transform(input)
      .cache()

    check3(output, input)
  }

  it should "fail if output column exists already, part G" in {
    // columns, outputCol exists => failed transform;
    // fail if both output colnames are empty or any exists already;
    // testOnly *QuantileThreshold* -- -z "column exists already"
    val input = inputDF.where("part = 'G'").selectExpr("uid", "score", "expected_index as c", "expected_rank as r")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0, 3.0))

    val model = estimator.fit(input)

    def failTransformSchema(): Unit = {
      assert(intercept[IllegalArgumentException] {
        model.setOutputCol("_c").setRankCol("r").transformSchema(input.schema)
      }.getMessage.contains("""Output column `r` already exists"""))

      assert(intercept[IllegalArgumentException] {
        model.setOutputCol("c").setRankCol("_r").transformSchema(input.schema)
      }.getMessage.contains("""Output column `c` already exists"""))

      assert(intercept[IllegalArgumentException] {
        model.setOutputCol("").setRankCol("").transformSchema(input.schema)
      }.getMessage.contains("""Output column name can't be empty"""))
    }

    def failTransform(): Unit = {
      assert(intercept[IllegalArgumentException] {
        model.setOutputCol("_c").setRankCol("r").transform(input)
      }.getMessage.contains("""Output column `r` already exists"""))

      assert(intercept[IllegalArgumentException] {
        model.setOutputCol("c").setRankCol("_r").transform(input)
      }.getMessage.contains("""Output column `c` already exists"""))

      assert(intercept[IllegalArgumentException] {
        model.setOutputCol("").setRankCol("").transform(input)
      }.getMessage.contains("""Output column name can't be empty"""))
    }

    failTransformSchema()
    failTransform()
  }


  it should "add columns for 3 output combinations, part F" in {
    // params: should add columns: class, rank, both;
    // testOnly *QuantileThreshold* -- -z "columns for 3"
    val input = inputDF.where("part = 'F'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setPriorValues(Array(1.0, 2.0, 3.0))

    val model = estimator.fit(input)

    def transformSchema(): Unit = {
        compareSchemas(
          model.setOutputCol("c").setRankCol("r").transformSchema(input.schema),
          input.schema
            .add(StructField("c", DataTypes.StringType))
            .add(StructField("r", DataTypes.DoubleType))
        )

      compareSchemas(
        model.setOutputCol("c").setRankCol("").transformSchema(input.schema),
        input.schema.add(StructField("c", DataTypes.StringType))
      )

      assert(intercept[IllegalArgumentException] {
        compareSchemas(
          model.setOutputCol("").setRankCol("r").transformSchema(input.schema),
          input.schema.add(StructField("r", DataTypes.DoubleType))
        )
      }.getMessage.contains("""requirement failed: Output column name can't be empty"""))

    }

    def transform(): Unit = {
      compareDataframes(
        model.setOutputCol("c").setRankCol("r").transform(input.drop("expected_index", "expected_rank")),
        input.withColumnRenamed("expected_index", "c").withColumnRenamed("expected_rank", "r")
      )

      assert(intercept[IllegalArgumentException] {
        compareDataframes(
          model.setOutputCol("").setRankCol("r").transform(input.drop("expected_index", "expected_rank")),
          input.drop("expected_index").withColumnRenamed("expected_rank", "r")
        )
      }.getMessage.contains("""requirement failed: Output column name can't be empty"""))

      compareDataframes(
        model.setOutputCol("c").setRankCol("").transform(input.drop("expected_index", "expected_rank")),
        input.withColumnRenamed("expected_index", "c").drop("expected_rank")
      )
    }

    transformSchema()
    transform()
  }

  it should "fail setting invalid prior parameter" in {
    // params: priorValues required len >= 2, min > 0;
    // testOnly *QuantileThreshold* -- -z "prior"

    val estimator = new ScoreQuantileThresholdEstimator()
      .setPriorValues(Array(0.00000001, 1.1))

    def priorSize(): Unit = {
      assert(intercept[IllegalArgumentException] {
        estimator.setPriorValues(Array(1.1))
      }.getMessage.contains("Prior size must be > 1"))

      assert(intercept[IllegalArgumentException] {
        estimator.set(estimator.priorValues, Array(1.2))
      }.getMessage.contains("priorValues given invalid value [1.2]"))
    }

    def priorMinValue(): Unit = {
      assert(intercept[IllegalArgumentException] {
        estimator.setPriorValues(Array(1.1, -0.00000001))
      }.getMessage.contains("min value must be > 0"))

      assert(intercept[IllegalArgumentException] {
        estimator.set(estimator.priorValues, Array(-0.1, 2.3))
      }.getMessage.contains("priorValues given invalid value [-0.1,2.3]"))
    }

    priorSize()
    priorMinValue()
  }

  it should "handle null values in grouping columns, part E" in {
    // testOnly *QuantileThreshold* -- -z "null values in grouping columns"
    val df = inputDF.where("part = 'E'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setGroupColumns(Seq("uid_type"))
      .setOutputCol("class_index")
      .setRankCol("rank")
      .setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(df)
    assert(model.groupsConfig.length === 2)
    assert(model.groupsConfig.map(_._1).toSet === Set("g/foo", "g/-"))

    val res = model.transform(df).cache()
    check3(res, df)
  }

  it should "take sample for each group, part D" in {
    // testOnly *QuantileThreshold* -- -z "sample for each group"
    val df = inputDF.where("part = 'D'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setGroupColumns(Seq("uid_type"))
      .setSampleSize(1)
      .setSampleRandomSeed(3)
      .setOutputCol("class_index")
      .setRankCol("rank")
      .setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(df.repartition(1).orderBy("uid"))
    assert(model.groupsConfig.length === 2)
    assert(model.groupsConfig.map(_._1).toSet === Set("g/foo", "g/-"))
    assert(model.groupsConfig.map(_._2.thresholds.last).toSet === Set(1.1, 2.1))

    val res = model.transform(df).cache()
    check3(res, df)
  }

  it should "fail if used columns not exists" in {
    //  columns, no inputCol or groupColumns => failed fit, transform;
    // testOnly *QuantileThreshold* -- -z "columns not exists"

    val df = spark.createDataFrame(Seq(
      ("a", 0.4, "A")
    )).toDF("uid", "score", "category")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score")
      .setGroupColumns(Seq("category", "uid"))
      .setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(df)

    def failFitNoInputCol(): Unit = {
      assert(intercept[IllegalArgumentException] {
        estimator.setInputCol("xs").fit(df)
      }.getMessage.contains("""Field "xs" does not exist"""))
    }

    def failFitNoGroupCol(): Unit = {
      assert(intercept[IllegalArgumentException] {
        estimator.setInputCol("score").setGroupColumns(Seq("group")).fit(df)
      }.getMessage.contains("""Field "group" does not exist"""))
    }

    def failTransformNoInputCol(): Unit = {
      assert(intercept[IllegalArgumentException] {
        model.setInputCol("xs").transform(df)
      }.getMessage.contains("""Field "xs" does not exist"""))
    }

    def failTransformNoGroupCol(): Unit = {
      assert(intercept[sql.AnalysisException] {
        model.setInputCol("score").setGroupColumns(Seq("group")).transform(df)
      }.getMessage.contains("""cannot resolve '`group`' given input columns: [uid, score, category]"""))
    }

    def failTransformSchemaNoInputCol(): Unit = {
      assert(intercept[IllegalArgumentException] {
        model.setInputCol("xs").setGroupColumns(Seq("category", "uid")).transformSchema(df.schema)
      }.getMessage.contains("""Field "xs" does not exist"""))
    }

    def failTransformSchemaNoGroupCol(): Unit = {
      assert(intercept[IllegalArgumentException] {
        model.setInputCol("score").setGroupColumns(Seq("group")).transformSchema(df.schema)
      }.getMessage.contains("""Field "group" does not exist"""))
    }

    failFitNoInputCol()
    failFitNoGroupCol()
    failTransformNoInputCol()
    failTransformNoGroupCol()
    failTransformSchemaNoInputCol()
    failTransformSchemaNoGroupCol()
  }

  it should "transform invalid score to null, part A" in {
    // testOnly *ScoreQuantile* -- -z "invalid score to null, part A"
    val input = inputDF.selectExpr("uid", "score_train", "score", "expected_index", "expected_rank").where("part = 'A'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train").setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 1) // one model fitted

    val output = model.setInputCol("score").setOutputCol("class_index").setRankCol("rank")
      .transform(input)
      .cache()

    check3(output, input)
  }

  it should "transform valid score to null if no fitted model, part B" in {
    // testOnly *ScoreQuantile* -- -z "valid score to null"
    val input = inputDF.selectExpr("uid", "score_train", "score", "expected_index", "expected_rank").where("part = 'B'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train").setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(input)
    assert(model.groupsConfig.isEmpty) // no models fitted

    val output = model.setInputCol("score").setOutputCol("class_index").setRankCol("rank")
      .transform(input)
      .cache()

    check3(output, input)
  }

  it should "not fail fitting on empty dataset" in {
    // testOnly *ScoreQuantile* -- -z "empty dataset"
    val input = inputDF.selectExpr("uid", "score_train", "score", "expected_index", "expected_rank").where("part = 'no data'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train").setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(input)
    assert(model.groupsConfig.isEmpty) // no models fitted
  }

  it should "not fail fitting on invalid data" in {
    // testOnly *ScoreQuantile* -- -z "fitting on invalid data"
    val input = inputDF.selectExpr("uid", "score_train", "score", "expected_index", "expected_rank").where("part = 'B'")

    val estimator = new ScoreQuantileThresholdEstimator()
      .setInputCol("score_train").setPriorValues(Seq(1.0, 1.0))

    val model = estimator.fit(input)
    assert(model.groupsConfig.isEmpty) // no models fitted
  }

  private def assertResult(accuracy: Int)(actual: DataFrame, expected: DataFrame): Assertion = {
    assert(expected.count() > 0)

    compareSchemas(
      actual.schema,
      expected.schema
        .add(StructField("class_index", DataTypes.StringType))
        .add(StructField("rank", DataTypes.DoubleType))
    )

    assert(actual.count === expected.count)

    compareDoubleColumns(actual, expected, "class_index", "expected_index", accuracy = 0)
    compareDoubleColumns(actual, expected, "rank", "expected_rank", accuracy = accuracy)

    compareDataframes(
      actual.drop("expected_rank", "expected_index"),
      expected.withColumnRenamed("expected_rank", "rank").withColumnRenamed("expected_index", "class_index"),
      accuracy = accuracy,
      unpersist = true
    )
  }

}

object ScoreQuantileThresholdTest extends should.Matchers {

  def createInputDF(spark: SparkSession): DataFrame = {
    // part, uid, uid_type, score_train, score, expected_index, expected_rank
    spark.createDataFrame(Seq(
      // smoke, part C
      InputRow("C", "a", "0.0", "foo", "1.000"),
      InputRow("C", "b", "1.0", "bar", "0.111"),
      InputRow("C", "c", "2.0", "bar", "0.778"),
      InputRow("C", "d", "3.0", "baz", "0.222"),
      InputRow("C", "e", "4.0", "baz", "0.963"),
      InputRow("C", "f", "5.0", "baz", "0.296"),
      InputRow("C", "g", "6.0", "qux", "0.167"),
      InputRow("C", "h", "7.0", "qux", "0.444"),
      InputRow("C", "i", "8.0", "qux", "0.722"),
      InputRow("C", "j", "9.0", "qux", "1.000"),
      // transform invalid score to null, part A
      InputRow("A", "a", "", "1", "2",    "1",    "0.5"), // one valid record
      InputRow("A", "b", "", "3", "null", "null", "null"),
      InputRow("A", "c", "", "4", "nan",  "null", "null"),
      // transform valid score to null if no fitted model, part B
      InputRow("B", "a", "", "",      "1",    "null", "null"),
      InputRow("B", "b", "", "null",  "2",    "null", "null"),
      InputRow("B", "c", "", "nan",   "3",    "null", "null"),
      // sample for each group, part D
      InputRow("D", "a", "foo",   "", "1.1", "2", "nan"),
      InputRow("D", "b", "foo",   "", "1.2", "2", "1"),
      InputRow("D", "c", "foo",   "", "1.3", "2", "1"),
      InputRow("D", "d", "null",  "", "2.1", "2", "nan"),
      InputRow("D", "e", "null",  "", "2.2", "2", "1"),
      InputRow("D", "f", "null",  "", "2.3", "2", "1"),
      // handle null values in grouping columns, part E
      InputRow("E", "a", "foo",   "", "1.1", "2", "nan"),
      InputRow("E", "b", "null",  "", "2.2", "2", "nan"),
      // add columns for 3 output combinations, part F
      InputRow("F", "a", "null",  "", "0.1", "3", "nan"),
      // fail if output column exists already, part G
      InputRow("G", "a", "null",  "", "0.4", "3", "1"),
      // fit-transform n groups dataset, part H
      InputRow("H", "a", "FOO", "",  "0", "3", "nan"),
      InputRow("H", "b", "BAR", "",  "1", "3", "nan"),
      InputRow("H", "c", "BAZ", "",  "2", "3", "nan"),
      // fit-transform one group dataset, part I
      InputRow("I", "a", "", "",  "0", "3", "nan"),
      InputRow("I", "b", "", "",  "1", "3", "nan"),
      InputRow("I", "c", "", "",  "2", "3", "nan"),
      // fit-transform no-groups dataset, part J
      InputRow("J", "a", "", "",  "0", "1", "1"),
      InputRow("J", "b", "", "",  "1", "2", "0.111"),
      InputRow("J", "c", "", "",  "2", "2", "0.778"),
      InputRow("J", "d", "", "",  "3", "3", "0.222"),
      InputRow("J", "e", "", "",  "4", "3", "0.963"),
      InputRow("J", "f", "", "",  "5", "3", "0.296"),
      InputRow("J", "g", "", "",  "6", "4", "0.167"),
      InputRow("J", "h", "", "",  "7", "4", "0.444"),
      InputRow("J", "i", "", "",  "8", "4", "0.722"),
      InputRow("J", "j", "", "",  "9", "4", "1"),
      // not fail if at least one group fitted; transform unknown groups to null, part K
      InputRow("K", "a", "OK",  "",     "0.1",  "null", "null"),
      InputRow("K", "b", "OK",  "nan",  "3.14", "null", "null"),
      InputRow("K", "c", "VK",  "nan",  "26",   "null", "null"),
      InputRow("K", "d", "VK",  "",     "28",   "null", "null"),
      InputRow("K", "e", "XZ",  "27",   "15",   "1",    "1"), // only one valid train record
      // fit with train vector.size=1, part M
      InputRow("M", "a", "null", "0.3", "0.1", "1", "1"),
      // transform: null => (null, null), nan => (null, null), part N
      InputRow("N", "a", "null",  "0.3", "0.1",  "1",    "1"),
      InputRow("N", "b", "null",  "0.7", "null", "null", "null"),
      InputRow("N", "c", "null",  "17",  "nan",  "null", "null")
    ))
    // part, uid, uid_type, score_train, score, expected_index, expected_rank
  }

  case class InputRow(
                       part: String,
                       uid: String,
                       uid_type: Option[String],
                       score_train: Option[Double],
                       score: Option[Double],
                       expected_index: Option[String],
                       expected_rank: Option[Double]
                     )

  object InputRow {
    import ColumnValueParser._

    def apply(part: String, uid: String, uid_type: String, score_train: String, score: String, expected_index: String, expected_rank: String): InputRow =
      InputRow(
        part = part,
        uid = uid,
        uid_type = parseStr(uid_type),
        score_train = parseDouble(score_train),
        score = parseDouble(score),
        expected_index = parseStr(expected_index),
        expected_rank = parseDouble(expected_rank)
      )

    def apply(part: String, uid: String, score: String, expected_index: String, expected_rank: String): InputRow =
      InputRow(
        part = part,
        uid = uid,
        uid_type = None,
        score_train = None,
        score = parseDouble(score),
        expected_index = parseStr(expected_index),
        expected_rank = parseDouble(expected_rank)
      )
  }

  private def c2s[T](xs: Iterable[T], collectionType: String = "Array"): String =
    s"${collectionType}(${xs.mkString(",")})"

  case class _ReferenceData(x: Array[Option[Double]], prior: Array[Double], class_index: Array[Option[Double]], rank: Array[Option[Double]]) {
    override def toString: String = s"_ReferenceData(\nx=${c2s(x)},\nclass_index=${c2s(class_index)},\nrank=${c2s(rank)},\nprior=${c2s(prior)})"
  }

  case class ReferenceData(x: Array[Double], prior: Array[Double], class_index: Array[String], rank: Array[Double]) {
    require(x.length == class_index.length && x.length == rank.length, "in/out vectors must be the same size")

    override def toString: String = s"ReferenceData(\n\tx=${c2s(x)},\n\tclass_index=${c2s(class_index)},\n\trank=${c2s(rank)},\n\tprior=${c2s(prior)})"

    def opt(i: Int): Option[Int] = if (i <= 0) None else Some(i)
    def opt(x: Double): Option[Double] = if (x.isNaN) None else Some(x)
    def opt(x: String): Option[String] = if (x.isEmpty || x == "null") None else Some(x)

    def toDataFrame(implicit spark: SparkSession): DataFrame =
      spark.createDataFrame(
        x.indices.map(idx => (s"uid_${idx}", x(idx), opt(class_index(idx)), opt(rank(idx))))
      ).toDF("uid", "x", "class_index", "rank")

  }

  private def loadReferenceData(resourceName: String): ReferenceData = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats: DefaultFormats = DefaultFormats

    val filePath = FileToolbox.getResourcePath(this, resourceName)
    val text = FileToolbox.loadTextFile(filePath)
    val _refdata = parse(text.replaceAllLiterally("NaN", "null")).extract[_ReferenceData]

    def o2d(xs: Array[Option[Double]]): Array[Double] = xs.map(_.getOrElse(Double.NaN))

    def o2i(xs: Array[Option[Double]]): Array[Int] = xs.map(x => {
      val res = x.getOrElse(Double.NaN)
      if (res.isNaN) 0 else res.toInt
    })

    def o2s(xs: Array[Option[Double]]): Array[String] = xs.map(x => {
      val res = x.getOrElse(Double.NaN)
      if (res.isNaN) "null" else res.toInt.toString
    })

    ReferenceData(
      x = o2d(_refdata.x),
      prior = _refdata.prior,
      class_index = o2s(_refdata.class_index),
      rank = o2d(_refdata.rank)
    )

  }

}
