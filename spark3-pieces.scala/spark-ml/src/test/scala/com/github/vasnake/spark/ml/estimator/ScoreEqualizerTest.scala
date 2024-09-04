/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.spark.ml.estimator

import com.github.vasnake.`ml-models`.complex.ScoreEqualizerConfig
import com.github.vasnake.common.file.FileToolbox
import com.github.vasnake.core.text.StringToolbox
import com.github.vasnake.spark.ml.estimator
import com.github.vasnake.spark.test._
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._
import scala.util.Random

class ScoreEqualizerTest
    extends AnyFlatSpec
       with should.Matchers
       with LocalSpark
       with DataFrameHelpers {
  import ScoreEqualizerTest._
  import spark.implicits._
  val check5: (DataFrame, DataFrame) => Assertion = assertResult(accuracy = 5)

  lazy val inputDF: DataFrame = cache(createInputDF(spark))

  it should "run smoke test" in {
    val input = inputDF.where("part = 'C'")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_raw")
      .setGroupColumns(Seq("uid_type", "uid"))
      .setSampleSize(10)
      .setSampleRandomSeed(2021.03)
      .setNumBins(7)
      .setNoiseValue(0.000001)
      .setEpsValue(0.0001)
      .setRandomValue(0.5)

    val model = estimator.fit(input)

    val res = model
      .setOutputCol("score")
      .transform(input)

    show(res, "output")
  }

  it should "execute toy example" in {
    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.3, 0.1, 0.0),
          ("b", 0.7, 3.14, 0.27968233),
          ("c", 13.0, 26.0, 0.74796144),
          ("d", 17.0, 28.0, 1.0),
          ("e", 27.0, 15.0, 0.4982242)
        )
      )
      .toDF("uid", "score_raw_train", "score_raw", "expected")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq.empty)
      .setSampleSize(1000)
      .setNumBins(10000)
      .setNoiseValue(1e-4)
      .setEpsValue(1e-3)
      .setRandomValue(0.5)
      .fit(df)

    // debug print
    val expectedConfig =
      """{
        |  "coefficients": [
        |    [
        |      0.0,
        |      0.0,
        |      0.2,
        |      0.4,
        |      0.6,
        |      0.8,
        |      1.0
        |    ],
        |    [
        |      0.0,
        |      0.0,
        |      0.38202460914831604,
        |      0.4358122025358817,
        |      0.7015049337832603,
        |      0.800000000000535,
        |      1.0
        |    ],
        |    [
        |      0.0,
        |      0.19387528695400894,
        |      0.28986193786194464,
        |      0.5594523251301159,
        |      0.6000000000016047,
        |      1.0,
        |      1.0
        |    ],
        |    [
        |      0.0,
        |      0.2,
        |      0.4,
        |      0.6,
        |      0.8,
        |      1.0,
        |      1.0
        |    ]
        |  ],
        |  "eps": 0.001,
        |  "intervals": [
        |    0.0,
        |    0.001,
        |    0.016455,
        |    0.4757729,
        |    0.6251235,
        |    0.999,
        |    0.999000000001,
        |    1.0
        |  ],
        |  "maxval": 27.0,
        |  "minval": 0.3,
        |  "noise": 0.0001,
        |  "random_value": 0.5
        |}
        |""".stripMargin
    // println(s"\nEQ config:\n`${eq.config}`;\nintervals: `${eq.config.intervals.mkString(", ")}`;\ncoefficients:\n${eq.config.coefficients.map(_.mkString(", ")).mkString(";\n")}")

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected")
    assert(res.count() === 5)
  }

  it should "produce reference scores" in {
    val df = spark
      .read
      .options(
        Map(
          "sep" -> ";",
          "header" -> "true",
          "timestampFormat" -> "yyyy-MM-dd HH:mm:ss"
        )
      )
      .csv(FileToolbox.getResourcePath(this, "/DM-7638-active-audience/scores-sample.csv"))
      .selectExpr(
        "uid",
        "cast(score_raw as double) as score_raw",
        "cast(score as double) as expected"
      )

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw")
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected")
    assert(res.count() === 101)
  }

  it should "transform invalid score to null" in {
    //  transform, null, nan => null, nan;

    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.3, Some(0.1), Some(0.0)),
          ("b", 0.7, Some(3.14), Some(0.27968233)),
          ("c", 13.0, None, None),
          ("d", 17.0, Some(Double.NaN), None),
          ("e", 27.0, Some(15.0), Some(0.4982242))
        )
      )
      .toDF("uid", "score_raw_train", "score_raw", "expected")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected")
    assert(res.count() === 5)
  }

  it should "work with train vector.size=2" in {
    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.3, 0.1, 8.300577e-08),
          ("b", 0.7, 3.14, 5.535685e-03),
          ("c", 13.0, 26.0, 1.0),
          ("d", 17.0, 28.0, 1.0),
          ("e", 27.0, 15.0, 1.0),
          ("aa", 14.0, 1.0, 0.0),
          ("bb", 0.03, 1.0, 0.0)
        )
      )
      .toDF("uid", "score_raw_train", "score_raw", "expected")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setRandomValue(0.5)
      .fit(df.where("uid in ('aa', 'bb')"))

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df.where("uid not in ('aa', 'bb')"))
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected")
    assert(res.count() === 5)
  }

  it should "work with train vector.size=3" in {
    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.3, 0.1, 0.0),
          ("b", 0.7, 3.14, 0.08354),
          ("c", 13.0, 26.0, 0.619903),
          ("d", 17.0, 28.0, 1.0),
          ("e", 27.0, 15.0, 0.43634),
          ("aa", 7.0, 0.0, 0.0),
          ("bb", 27.0, 0.0, 0.0),
          ("cc", 1.0, 0.0, 0.0)
        )
      )
      .toDF("uid", "score_raw_train", "score_raw", "expected")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setRandomValue(0.5)
      .fit(df.where("length(uid) > 1"))

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df.where("length(uid) < 2"))
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected", accuracy = 6)
    assert(res.count() === 5)
  }

  it should "do more reference scores" in {
    val df = buildDF(
      train = "99, 55, 1, 2, 3, 73, 95, 91, 97",
      score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
      expected =
        "0.0, 0.223051, 0.292044, 0.293951, 0.272982, 0.297835, 0.544477, 0.478477, 0.243779"
    )

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected", accuracy = 6)
    assert(res.count() === 9)
  }

  it should "add random noise" in {
    val df = spark
      .read
      .options(
        Map(
          "sep" -> ";",
          "header" -> "true",
          "timestampFormat" -> "yyyy-MM-dd HH:mm:ss"
        )
      )
      .schema("uid STRING, score_raw DOUBLE, score DOUBLE")
      .csv(FileToolbox.getResourcePath(this, "/DM-7638-active-audience/scores-sample.csv"))
      .selectExpr(
        "uid",
        "cast(score_raw as double) as score_raw",
        "cast(score as double) as expected"
      )

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw")
      .fit(df)

    val res = eq
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    assert(res.count() === 101)

    val diff = res.selectExpr("uid", "abs(score - expected) as diff").where("diff > 0.01")
    assert(diff.count() === 0)
  }

  it should "not fail if no fitted groups" in {
    //  fit, no models => failed fit;

    val df = spark
      .createDataFrame(
        Seq(
          ("a", Some(Double.NaN), 33.0, 33.0) // invalid input
        )
      )
      .toDF("uid", "score_raw_train", "score_raw", "expected")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .fit(df)

    assert(eq.groupsConfig.isEmpty)
  }

  it should "not fail on fit if at least one group fitted, transform unknown groups to null" in {
    //  fit, no valid data => no model;
    //  transform, unknown group => drop rows;

    val df = spark
      .createDataFrame(
        Seq(
          ("e", "XZ", Some(27.0), 15.0, Some(0.0)), // only one valid train record
          ("a", "OK", None, 0.1, None),
          ("b", "OK", Some(Double.NaN), 3.14, None),
          ("c", "VK", Some(Double.NaN), 26.0, None),
          ("d", "VK", None, 28.0, None)
        )
      )
      .toDF("uid", "uid_type", "score_raw_train", "score_raw", "expected")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq("uid_type"))
      .setRandomValue(0.5)
      .fit(df)

    assert(eq.groupsConfig.length === 1)
    assert(eq.groupsConfig.head._1 === "g/XZ") // just one fitted model

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    assert(res.count() === 5)
    compareSchemas(res.schema, df.schema.add(StructField("score", DataTypes.DoubleType)))
    compareDoubleColumns(res, df, "score", "expected")
  }

  it should "equalize one-column groups" in {
    val df = {
      import org.apache.spark.sql.{ functions => sf }

      val df1 = spark
        .createDataFrame(
          Seq(
            ("a", "OK", 0.3, 0.1, 0.0),
            ("b", "OK", 0.7, 3.14, 0.27968233),
            ("c", "OK", 13.0, 26.0, 0.74796144),
            ("d", "OK", 17.0, 28.0, 1.0),
            ("e", "OK", 27.0, 15.0, 0.4982242)
          )
        )
        .toDF("uid", "uid_type", "score_raw_train", "score_raw", "expected")

      val df2 = buildDF(
        train = "99, 55, 1, 2, 3, 73, 95, 91, 97",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected =
          "0.0, 0.223051, 0.292044, 0.293951, 0.272982, 0.297835, 0.544477, 0.478477, 0.243779"
      ).withColumn("uid_type", sf.lit("VK"))
        .select("uid", "uid_type", "score_raw_train", "score_raw", "expected")

      df1 union df2
    }

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq("uid_type"))
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected", accuracy = 6)
    assert(res.count() === 14)
  }

  it should "equalize two-column groups" in {
    val df = {
      val df1 = spark
        .createDataFrame(
          Seq(
            ("a", "OK", "foo", 0.3, 0.1, 0.0),
            ("b", "OK", "foo", 0.7, 3.14, 0.27968233),
            ("c", "OK", "foo", 13.0, 26.0, 0.74796144),
            ("d", "OK", "foo", 17.0, 28.0, 1.0),
            ("e", "OK", "foo", 27.0, 15.0, 0.4982242)
          )
        )
        .toDF("uid", "uid_type", "category", "score_raw_train", "score_raw", "expected")

      val df2 = buildDF(
        train = "99, 55, 1, 2, 3, 73, 95, 91, 97",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected =
          "0.0, 0.223051, 0.292044, 0.293951, 0.272982, 0.297835, 0.544477, 0.478477, 0.243779"
      ).selectExpr(
        "uid",
        "'VK' as uid_type",
        "'bar' as category",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      val df3 = buildDF(
        train = "19, 25, 31, 42, 53, 63, 75, 81, 97",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected = "0.0, 0.0, 0.130266, 0.170497, 0.0, 0.246967, 0.816186, 0.763845, 0.0"
      ).selectExpr(
        "uid",
        "'OK' as uid_type",
        "'bar' as category",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      val df4 = buildDF(
        train = "91, 52, 13, 24, 35, 76, 97, 98, 99",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected = "0, 0, 0.131936, 0.153365, 0.006922, 0.204792, 0.544612, 0.467587, 0"
      ).selectExpr(
        "uid",
        "'VK' as uid_type",
        "'foo' as category",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      df1 union df2 union df3 union df4
    }

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq("category", "uid_type"))
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected", accuracy = 6)
    assert(res.count() === 32)
  }

  it should "equalize n-column groups" in {
    val df = {
      val df1 = spark
        .createDataFrame(
          Seq(
            ("a", "OK", "foo", "a1", 0.3, 0.1, 0.0),
            ("b", "OK", "foo", "a1", 0.7, 3.14, 0.27968233),
            ("c", "OK", "foo", "a1", 13.0, 26.0, 0.74796144),
            ("d", "OK", "foo", "a1", 17.0, 28.0, 1.0),
            ("e", "OK", "foo", "a1", 27.0, 15.0, 0.4982242)
          )
        )
        .toDF(
          "uid",
          "uid_type",
          "category",
          "audience_name",
          "score_raw_train",
          "score_raw",
          "expected"
        )

      val df2 = buildDF(
        train = "99, 55, 1, 2, 3, 73, 95, 91, 97",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected =
          "0.0, 0.223051, 0.292044, 0.293951, 0.272982, 0.297835, 0.544477, 0.478477, 0.243779"
      ).selectExpr(
        "uid",
        "'VK' as uid_type",
        "'bar' as category",
        "'a1' as audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      val df3 = buildDF(
        train = "19, 25, 31, 42, 53, 63, 75, 81, 97",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected = "0.0, 0.0, 0.130266, 0.170497, 0.0, 0.246967, 0.816186, 0.763845, 0.0"
      ).selectExpr(
        "uid",
        "'OK' as uid_type",
        "'bar' as category",
        "'a1' as audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      val df4 = buildDF(
        train = "91, 52, 13, 24, 35, 76, 97, 98, 99",
        score_raw = "0.1, 3.14, 26, 28, 15, 33, 90, 80, 7",
        expected = "0, 0, 0.131936, 0.153365, 0.006922, 0.204792, 0.544612, 0.467587, 0"
      ).selectExpr(
        "uid",
        "'VK' as uid_type",
        "'foo' as category",
        "'a1' as audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      val df5 = buildDF(
        train = "1, 5.2, 1.3, 2.4, 3.5, 7.6, 9.7, 9.8, 9.9, 0.33",
        score_raw = "0.1, 3.14, 2.6, 2.8, 1.5, 3.3, 9.0, 0.8, 7.1, 0.1",
        expected =
          "0, 0.370273, 0.318738, 0.338083, 0.227467, 0.384274, 0.641999, 0.057494, 0.57888, 0"
      ).selectExpr(
        "uid",
        "'VK' as uid_type",
        "'foo' as category",
        "'a2' as audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )

      df1 union df2 union df3 union df4 union df5
    }

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq("category", "uid_type", "audience_name"))
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected", accuracy = 6)
    assert(res.count() === 42)
  }

  it should "handle null values in grouping columns" in {
    val df = spark
      .createDataFrame(
        Seq(
          ("a", Some("OK"), Some("foo"), Some(1), 0.3, 0.1, 0.0),
          ("b", Some("OK"), Some("foo"), Some(1), 0.7, 3.14, 0.4013531538886407),
          ("c", Some("OK"), Some("foo"), Some(1), 13.0, 26.0, 0.6127097436149826),
          ("d", Some("OK"), Some("foo"), Some(1), 17.0, 28.0, 1.0),
          ("e", Some("OK"), Some("foo"), Some(1), 27.0, 15.0, 0.499192817215795),
          ("a", Some("OK"), Some("foo"), Some(1), 0.3, 0.1, 0.0),
          ("b", None, Some("foo"), Some(1), 0.7, 3.14, 0.01021589871016548),
          ("c", None, Some("foo"), Some(1), 13.0, 26.0, 1.0),
          ("d", None, Some("foo"), Some(1), 17.0, 28.0, 1.0),
          ("e", Some("OK"), Some("foo"), Some(1), 27.0, 15.0, 0.499192817215795),
          ("a", Some("OK"), Some("foo"), Some(1), 0.3, 0.1, 0.0),
          ("b", None, None, Some(1), 0.7, 3.14, 0.002252295870821873),
          ("c", None, Some("foo"), Some(1), 13.0, 26.0, 1.0),
          ("d", None, None, Some(1), 17.0, 28.0, 1.0),
          ("e", Some("OK"), Some("foo"), Some(1), 27.0, 15.0, 0.499192817215795),
          ("a", Some("OK"), Some("foo"), Some(1), 0.3, 0.1, 0.0),
          ("b", None, None, Some(1), 0.7, 3.14, 0.002252295870821873),
          ("c", None, Some("foo"), None, 13.0, 26.0, 1.0),
          ("d", None, None, None, 17.0, 28.0, 1.0),
          ("e", Some("OK"), Some("foo"), Some(1), 27.0, 15.0, 0.499192817215795)
        )
      )
      .toDF(
        "uid",
        "uid_type",
        "category",
        "audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq("category", "uid_type", "audience_name"))
      .setRandomValue(0.5)
      .fit(df)

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)
      .cache()

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected", accuracy = 6)
    assert(res.count() === 20)
  }

  it should "take sample w/o groups" in {
    val df = spark
      .createDataFrame(
        Seq(
          ("a", "OK", "foo", "a1", 0.3, 0.1, 0.0),
          ("b", "OK", "foo", "a1", 0.7, 3.14, 0.27968233),
          ("c", "OK", "foo", "a1", 13.0, 26.0, 0.74796144),
          ("d", "OK", "foo", "a1", 17.0, 28.0, 1.0),
          ("e", "OK", "foo", "a1", 27.0, 15.0, 0.4982242)
        )
      )
      .toDF(
        "uid",
        "uid_type",
        "category",
        "audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setSampleRandomSeed(0.5)
      .setSampleSize(1)
      .fit(df)

    assert(eq.groupsConfig.head._2.intervals.length === 5)
  }

  it should "take sample for each group" in {
    val input = spark
      .createDataFrame(
        Seq(
          ("a", "OK", "foo", "a1", 0.3, 0.1, 0.0),
          ("b", "OK", "foo", "a1", 0.7, 3.14, 0.27968233),
          ("b", "OK", "foo", "a1", 0.9, 3.14, 0.27968233),
          ("b", "OK", "foo", "a1", 1.0, 3.14, 0.27968233),
          ("c", "VK", "foo", "a1", 13.0, 26.0, 0.74796144),
          ("d", "VK", "foo", "a1", 17.0, 28.0, 1.0),
          ("d", "VK", "foo", "a1", 19.0, 28.0, 1.0),
          ("d", "VK", "foo", "a1", 20.0, 28.0, 1.0)
        )
      )
      .toDF(
        "uid",
        "uid_type",
        "category",
        "audience_name",
        "score_raw_train",
        "score_raw",
        "expected"
      )
      .repartition(2, $"uid_type")

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setGroupColumns(Seq("category", "uid_type", "audience_name"))
      .setSampleSize(1)
      .setSampleRandomSeed(1)
      .fit(input)

    assert(eq.groupsConfig.toMap.apply("g/foo/VK/a1").intervals.length === 6)
    assert(eq.groupsConfig.toMap.apply("g/foo/OK/a1").intervals.length === 4)
  }

  it should "issue a warning while fit on single unique value (WIP)" in {
    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.3, Some(0.1), Some(0.0))
        )
      )
      .toDF("uid", "score_raw_train", "score_raw", "expected")

    // TODO: rewrite log listener using log4j v1 interface
    // val lg = new LogGuard()

    val eq = new estimator.ScoreEqualizerEstimator()
      .setInputCol("score_raw_train")
      .setRandomValue(0.5)
      .fit(df)

    // assert(lg.getMessages.exists(_.contains("'X' contains single unique value, so equalization cannot be done properly")))
    // lg.close()

    val res = eq
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(df)

    val expectedSchema = df.schema.add(StructField("score", DataTypes.DoubleType))
    compareSchemas(res.schema, expectedSchema)
    compareColumns(res, "score", "expected")
    assert(res.count() === 1)
  }

  it should "fail if input columns not exists" in {
    //  columns, no inputCol or groupColumns => failed fit, transform;

    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.4, "A")
        )
      )
      .toDF("uid", "score_raw", "category")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_raw")
      .setGroupColumns(Seq("category", "uid"))
      .setOutputCol("score")

    val model = estimator.fit(df)

    def failFitNoInputCol(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.setInputCol("xs").fit(df)
      }
      assert(ex.getMessage.contains("""Field "xs" does not exist"""))
    }

    def failFitNoGroupCol(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.setInputCol("score_raw").setGroupColumns(Seq("group")).fit(df)
      }
      assert(ex.getMessage.contains("""Field "group" does not exist"""))
    }

    def failTransformNoInputCol(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        model.setInputCol("xs").transform(df)
      }
      assert(ex.getMessage.contains("""Field "xs" does not exist"""))
    }

    def failTransformNoGroupCol(): Unit = {
      val ex = intercept[sql.AnalysisException] {
        model.setInputCol("score_raw").setGroupColumns(Seq("group")).transform(df)
      }
      assert(
        ex.getMessage
          .contains("""cannot resolve '`group`' given input columns: [uid, score_raw, category]""")
      )
    }

    def failTransformSchemaNoInputCol(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        model.setInputCol("xs").setGroupColumns(Seq("category", "uid")).transformSchema(df.schema)
      }
      assert(ex.getMessage.contains("""Field "xs" does not exist"""))
    }

    def failTransformSchemaNoGroupCol(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        model.setInputCol("score_raw").setGroupColumns(Seq("group")).transformSchema(df.schema)
      }
      assert(ex.getMessage.contains("""Field "group" does not exist"""))
    }

    failFitNoInputCol()
    failFitNoGroupCol()
    failTransformNoInputCol()
    failTransformNoGroupCol()
    failTransformSchemaNoInputCol()
    failTransformSchemaNoGroupCol()
  }

  it should "fail if output column exists already" in {
    //  columns, outputCol exists => failed transform;

    val df = spark
      .createDataFrame(
        Seq(
          ("a", 0.4, "A")
        )
      )
      .toDF("uid", "score_raw", "category")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_raw")
      .setGroupColumns(Seq("category", "uid"))
      .setOutputCol("score_raw")

    val model = estimator.fit(df)

    def failTransformSchema(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        model.transformSchema(df.schema)
      }
      assert(ex.getMessage.contains("""Output column `score_raw` already exists"""))
    }

    def failTransform(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        model.transform(df)
      }
      assert(ex.getMessage.contains("""Output column `score_raw` already exists"""))
    }

    failTransformSchema()
    failTransform()
  }

  it should "fail on invalid params" in {
    //  invalid params, require sampleSize > 0, numBins > 0, randomValue in [0,1] +: nan

    val estimator = new ScoreEqualizerEstimator()

    def failSetSampleSize(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.setSampleSize(0)
      }
      assert(ex.getMessage.contains("Sample size must be a positive number"))
    }

    def failSetSampleSize2(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.set(estimator.sampleSize, -1)
      }
      assert(ex.getMessage.contains("parameter sampleSize given invalid value -1"))
    }

    def failSetNumBins(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.setNumBins(0)
      }
      assert(ex.getMessage.contains("Num bins must be a positive number"))
    }

    def failSetNumBins2(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.set(estimator.numBins, -1)
      }
      assert(ex.getMessage.contains("parameter numBins given invalid value -1"))
    }

    def failSetRandomValue(): Unit = assert(intercept[IllegalArgumentException] {
      estimator.setRandomValue(-0.0001)
    }.getMessage.contains("Random value must be in range [0, 1]"))

    def failSetRandomValue2(): Unit = {
      val ex = intercept[IllegalArgumentException] {
        estimator.set(estimator.randomValue, 1.0001)
      }
      assert(ex.getMessage.contains("parameter randomValue given invalid value 1.0001"))
    }

    failSetSampleSize()
    failSetSampleSize2()
    failSetNumBins()
    failSetNumBins2()
    failSetRandomValue()
    failSetRandomValue2()
  }

  it should "transform invalid score to null, part A" in {
    val input =
      inputDF.selectExpr("uid", "score_train", "score_raw", "expected").where("part = 'A'")
    show(input, message = "input")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_train")
      .setRandomValue(0.5)

    val model = estimator.fit(input)
    assert(model.groupsConfig.length === 1) // one model fitted

    val output = model
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(input)
      .cache()

    show(output, message = "output")
    check5(output, input)
  }

  it should "transform valid score to null if no fitted model, part B" in {
    val input =
      inputDF.selectExpr("uid", "score_train", "score_raw", "expected").where("part = 'B'")
    show(input, message = "input")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_train")
      .setRandomValue(0.5)

    val model = estimator.fit(input)
    assert(model.groupsConfig.isEmpty) // no models fitted

    val output = model
      .setInputCol("score_raw")
      .setOutputCol("score")
      .transform(input)
      .cache()

    show(output, message = "output")
    check5(output, input)
  }

  it should "not fail fitting on empty dataset" in {
    val input =
      inputDF.selectExpr("uid", "score_train", "score_raw", "expected").where("part = 'no data'")
    show(input, message = "input")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_train")
      .setRandomValue(0.5)

    val model = estimator.fit(input)
    assert(model.groupsConfig.isEmpty) // no models fitted
  }

  it should "not fail fitting on invalid data" in {
    val input =
      inputDF.selectExpr("uid", "score_train", "score_raw", "expected").where("part = 'B'")
    show(input, message = "input")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_train")
      .setRandomValue(0.5)

    val model = estimator.fit(input)
    assert(model.groupsConfig.isEmpty) // no models fitted
  }

  it should "not fail on many groups" in {
    // TODO: try to reproduce failed job environment (current setup didn't show any failures)
    //  there are ways:
    // export JAVA_OPTS="-XX:MaxMetaspaceSize=500m -Xss200k -Xms500m -Xmx1G"
    // export extraJavaOptions="-XX:MaxMetaspaceSize=500m -Xss200k -Xms500m -Xmx1G"
    //    --conf "spark.executor.extraJavaOptions=${extraJavaOptions}" \
    //    --conf "spark.driver.extraJavaOptions=${extraJavaOptions}" \
    // ... https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/tooldescr007.html

    val input = cache(createMassiveInputDF(spark, partsNum = 100, scoresNum = 10))
    show(input, "input")

    val estimator = new ScoreEqualizerEstimator()
      .setInputCol("score_raw")
      .setGroupColumns(Seq("part"))
      .setSampleSize(100000)
      .setNumBins(10000)
      .setNoiseValue(1e-4)
      .setEpsValue(1e-3)
    // .setSampleRandomSeed(2021.09)
    // .setRandomValue(0.5)

    val model = estimator.fit(input)

    val res = model
      .setOutputCol("score_equalized")
      .transform(input)

    show(res, "output")
  }

  private def assertResult(accuracy: Int)(actual: DataFrame, expected: DataFrame): Assertion = {
    assert(expected.count() > 0)

    compareSchemas(
      actual.schema,
      expected
        .schema
        .add(StructField("score", DataTypes.DoubleType))
    )

    assert(actual.count === expected.count)

    compareDoubleColumns(actual, expected, "score", "expected", accuracy = accuracy)

    compareDataframes(
      actual.drop("expected"),
      expected.withColumnRenamed("expected", "score"),
      accuracy = accuracy,
      unpersist = true
    )
  }
}

object ScoreEqualizerTest extends should.Matchers {
  val randomGen: Random = new scala.util.Random()

  def createMassiveInputDF(
    spark: SparkSession,
    partsNum: Int,
    scoresNum: Int
  ): DataFrame = {
    // part, score_raw

    def randomScore = randomGen.nextDouble()

    def rows: Seq[String] = for {
      part <- 1 to partsNum
      _ <- 1 to scoresNum
    } yield s"${part},${randomScore}"

    spark.createDataFrame(rows.map(InputRow(_))).selectExpr("part", "score_raw")
  }

  def createInputDF(spark: SparkSession): DataFrame =
    // part, uid, uid_type, score_train, score_raw, expected
    spark.createDataFrame(
      Seq(
        // smoke, part C
        InputRow("C", "a", "foo", "", "0.4", ""),
        InputRow("C", "b", "bar", "", "0.5", ""),
        InputRow("C", "c", "baz", "", "0.6", ""),
        // part A, transform invalid score to null
        InputRow("A", "a", "", "1", "2", "0.109105"),
        InputRow("A", "b", "", "3", "null", "null"),
        InputRow("A", "c", "", "4", "nan", "null"),
        // transform valid score to null if no fitted model, part B
        InputRow("B", "a", "", "", "1", "null"),
        InputRow("B", "b", "", "null", "2", "null"),
        InputRow("B", "c", "", "nan", "3", "null")
      )
    )
    // part, uid, uid_type, score_train, score_raw, expected

  case class InputRow(
    part: String,
    uid: String,
    uid_type: Option[String],
    score_train: Option[Double],
    score_raw: Option[Double],
    expected: Option[Double]
  )
  object InputRow {
    import ColumnValueParser._

    def apply(
      part: String,
      uid: String,
      uid_type: String,
      score_train: String,
      score_raw: String,
      expected: String
    ): InputRow =
      InputRow(
        part = part,
        uid = uid,
        uid_type = parseStr(uid_type),
        score_train = parseDouble(score_train),
        score_raw = parseDouble(score_raw),
        expected = parseDouble(expected)
      )

    def apply(row: String): InputRow = row.split(",").map(_.trim).toList match {
      case group :: score :: Nil =>
        InputRow(
          part = group,
          uid = "",
          uid_type = None,
          score_train = None,
          score_raw = parseDouble(score),
          expected = None
        )
      case _ => sys.error(s"Only 2-items CSV supported, got `${row}`")
    }
  }

  // TODO: setup log capture for sl4j v1
  // class LogGuard extends AutoCloseable {
  //  import ch.qos.logback.classic.Logger
  //  import ch.qos.logback.classic.spi.ILoggingEvent
  //  import ch.qos.logback.core.read.ListAppender
  //  import org.slf4j.LoggerFactory
  //
  //  val logger: Logger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
  //  val listAppender = new ListAppender[ILoggingEvent]
  //  logger.addAppender(listAppender)
  //  listAppender.start()
  //
  //  override def close(): Unit = {
  //    listAppender.stop()
  //    listAppender.list.clear()
  //    logger.detachAppender(listAppender)
  //  }
  //
  //  def getMessages: Array[String] = {
  //    import scala.collection.JavaConverters._
  //    listAppender.list.asScala.map(e => e.getMessage).toArray
  //  }
  // }

  def buildDF(
    train: String,
    score_raw: String,
    expected: String
  )(implicit
    spark: SparkSession
  ): DataFrame = {
    import StringToolbox._
    implicit val sep = Separators(",")

    val t: Seq[Double] = train.s2list.map(_.toDouble)
    val s: Seq[Double] = score_raw.s2list.map(_.toDouble)
    val e: Seq[Double] = expected.s2list.map(_.toDouble)
    require(t.length == s.length && t.length == e.length)

    spark
      .createDataFrame(t.indices.map { idx =>
        (s"uid_${idx}", t(idx), s(idx), e(idx))
      })
      .toDF("uid", "score_raw_train", "score_raw", "expected")
  }

  object debuggingTools {
    private def generateSample(spark: SparkSession): Unit = {

      def scoreRaw(n: Int): Double = n.toDouble * math.random % 2.0

      val sourceDF = spark
        .createDataFrame(
          (0 to 100).map(n => (s"uid_${n}", scoreRaw(n)))
        )
        .toDF("uid", "score_raw")

      sourceDF
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .options(Map("sep" -> ";", "header" -> "true"))
        .csv("/tmp/DM-7638-active-audience/eq-input-sample")
    }

    private def configFromResource(resourcePath: String) = {
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val formats: DefaultFormats = DefaultFormats

      val text = FileToolbox.loadTextFile(FileToolbox.getResourcePath(this, resourcePath))

      parse(text).extract[ScoreEqualizerConfig]
    }
  }
}
