/**
 * Created by vasnake@gmail.com on 2024-08-14
 */
package com.github.vasnake.spark.app.datasets

import com.github.vasnake.spark.test.{LocalSpark, SimpleLocalSpark}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec._
import org.scalatest.matchers._
import com.github.vasnake.spark.app.datasets.JoinerAppAggregationTest.{avgAggregator, maxAggregator, minAggregator, pipelines}
import com.github.vasnake.spark.app.datasets.joiner.EtlFeatures.{DomainAggregationConfig, defaultAggregationConfig}

class JoinerAppAggregationTest  extends AnyFlatSpec with should.Matchers  with SimpleLocalSpark {

  import spark.implicits._
  import EtlFeaturesFunctionsTest._
  import joiner._
  import joiner.implicits._

  it should "perform default aggregation" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=1;b=2;c=3, aFeature: 9;8;7, pFeature2: 2, pFeature3: 3")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
      "cast(mFeature as map<string, float>) foo; " +
      "cast(aFeature as array<float>) bar; " +
      "cast(pFeature1 as float) baz_f1; " +
      "cast(pFeature2 as float) baz_f2; " +
      "cast(pFeature3 as float) baz_f3", ";"
    )

    val cfg: Map[String, DomainAggregationConfig] = Seq("foo", "bar", "baz").map(n => (n, defaultAggregationConfig)).toMap
    val expected = Seq("[42,Map(b -> 2.0, a -> 1.0, c -> 3.0),WrappedArray(9.0, 8.0, 7.0),null,2.0,3.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "aggregate prefix_type domain" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, pFeature2: 21, pFeature3: 31"),
      SourceRow("uid: 42"),
      SourceRow("uid: 42, pFeature2: 23, pFeature3: 33"),

      SourceRow("uid: 24, pFeature2: 241, pFeature3: 231"),
      SourceRow("uid: 24"),
      SourceRow("uid: 24, pFeature2: 243, pFeature3: 233")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(pFeature1 as float) foo_f1; " +
        "cast(pFeature2 as float) foo_f2; " +
        "cast(pFeature3 as float) foo_f3",
      ";"
    )
    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map("foo" -> defaultAggregationConfig)
    val expected = Seq(
      "[42,null,22.0,32.0]",
      "[24,null,242.0,232.0]"
    )

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "aggregate array_type domain" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, aFeature: 19;18;17"),
      SourceRow("uid: 42, aFeature: ;;"),
      SourceRow("uid: 42, aFeature: 39;38;37")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(aFeature as array<float>) foo; ",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map("foo" -> defaultAggregationConfig)
    val expected = Seq("[42,WrappedArray(29.0, 28.0, 27.0)]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "aggregate map_type domain" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=11;b=21;c=31"),
      SourceRow("uid: 42, mFeature: a=0"),
      SourceRow("uid: 42, mFeature: a=13;b=23;c=33")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map("foo" -> defaultAggregationConfig)
    val expected = Seq("[42,Map(b -> 22.0, a -> 8.0, c -> 32.0)]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "aggregate dataset with all kind of domains" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=11;b=12;c=13, aFeature: 19;18;17, pFeature2: 21, pFeature3: 31"),
      SourceRow("uid: 42, mFeature: a=21;b=22;c=23, aFeature: 29;28;27, pFeature2: 22, pFeature3: 32"),
      SourceRow("uid: 42, mFeature: a=31;b=32;c=33, aFeature: 39;38;37, pFeature2: 23, pFeature3: 33")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> defaultAggregationConfig,
      "bar" -> defaultAggregationConfig,
      "baz" -> defaultAggregationConfig
    )
    val expected = Seq("[42,Map(b -> 22.0, a -> 21.0, c -> 23.0),WrappedArray(29.0, 28.0, 27.0),null,22.0,32.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "aggregate partition with null-ed domains" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42"),
      SourceRow("uid: 42"),
      SourceRow("uid: 42"),
      SourceRow("uid: 24"),
      SourceRow("uid: 24, mFeature: a=11;b=12;c=13, aFeature: 19;18;17, pFeature2: 12, pFeature3: 13"),
      SourceRow("uid: 24, mFeature: a=31;b=32;c=33, aFeature: 39;38;37, pFeature2: 32, pFeature3: 33"),
      SourceRow("uid: 24")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> defaultAggregationConfig,
      "bar" -> defaultAggregationConfig,
      "baz" -> defaultAggregationConfig
    )
    val expected = Seq(
      "[42,null,null,null,null,null]",
      "[24,Map(b -> 22.0, a -> 21.0, c -> 23.0),WrappedArray(29.0, 28.0, 27.0),null,22.0,23.0]"
    )

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "aggregate domains with null features" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=11;b=12, aFeature: 19;18;, pFeature2: 21"),
      SourceRow("uid: 42, mFeature: a=21;c=23, aFeature: 29;;27, pFeature3: 32"),
      SourceRow("uid: 42, mFeature: b=32;c=33, aFeature: ;38;37")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> defaultAggregationConfig,
      "bar" -> defaultAggregationConfig,
      "baz" -> defaultAggregationConfig
    )
    val expected = Seq("[42,Map(b -> 22.0, a -> 16.0, c -> 28.0),WrappedArray(24.0, 28.0, 32.0),null,21.0,32.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "apply domain-specified aggregation pipeline for each domain" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: b=12;c=13, aFeature: 19;18;17, pFeature2:21, pFeature3:31"),
      SourceRow("uid: 42, mFeature: a=21;c=23, aFeature: 29;;27"),
      SourceRow("uid: 42, mFeature: a=31;b=32, aFeature: 39;38;37, pFeature2:23, pFeature3:33")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> maxAggregator),
      "bar" -> Map("domain" -> minAggregator),
      "baz" -> Map("domain" -> maxAggregator)
    )
    val expected = Seq("[42,Map(b -> 32.0, a -> 31.0, c -> 23.0),WrappedArray(19.0, 18.0, 17.0),null,23.0,33.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "apply feature-specified aggregation pipeline for specified features" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=11;b=12;c=13, aFeature: 19;18;17, pFeature2:21, pFeature3:31"),
      SourceRow("uid: 42, mFeature: a=21;b=22;c=23, aFeature: 29;28;27, pFeature2:22, pFeature3:32"),
      SourceRow("uid: 42, mFeature: a=31;b=32;c=33, aFeature: 39;38;37, pFeature2:23, pFeature3:33")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> avgAggregator, "a" -> minAggregator, "b" -> maxAggregator),
      "bar" -> Map("domain" -> minAggregator, "0" -> avgAggregator, "1" -> maxAggregator),
      "baz" -> Map("domain" -> maxAggregator, "baz_f3" -> minAggregator)
    )
    val expected = Seq("[42,Map(b -> 32.0, a -> 11.0, c -> 23.0),WrappedArray(29.0, 38.0, 17.0),null,23.0,31.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "apply pipelines with different functions" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=11;b=12;c=13, aFeature: 19;18;17, pFeature2:21, pFeature3:31"),
      SourceRow("uid: 42, mFeature: a=21;b=22;c=23, aFeature: 29;28;27, pFeature2:22, pFeature3:32"),
      SourceRow("uid: 42, mFeature: a=31;b=32;c=33, aFeature: 39;38;37, pFeature2:23, pFeature3:33")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

    //df.printSchema()
    //df.show(truncate = false)

    // TODO: add test for every implemented function
    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> pipelines.avg, "a" -> pipelines.min, "b" -> pipelines.max),
      "bar" -> Map("domain" -> pipelines.min, "0" -> pipelines.avg, "1" -> pipelines.max),
      "baz" -> Map("domain" -> pipelines.avg, "baz_f2" -> pipelines.max, "baz_f3" -> pipelines.min)
    )

    val expected = Seq("[42,Map(b -> 32.0, a -> 11.0, c -> 23.0),WrappedArray(29.0, 38.0, 17.0),null,23.0,31.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  // testOnly *EtlFeaturesAgg* -- -z "sum function"
  it should "aggregate domains using sum function" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=11;b=12, aFeature: 19;18;, pFeature2: 21"),
      SourceRow("uid: 42, mFeature: a=21;c=23, aFeature: 29;;27, pFeature3: 32"),
      SourceRow("uid: 42, mFeature: b=32;c=33, aFeature: ;38;37, pFeature2: 23")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

//    df.printSchema()
//    df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> pipelines.sum),
      "bar" -> Map("domain" -> pipelines.sum),
      "baz" -> Map("domain" -> pipelines.sum)
    )
    val expected = Seq("[42,Map(b -> 44.0, a -> 32.0, c -> 56.0),WrappedArray(48.0, 56.0, 64.0),null,44.0,32.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

//    res.printSchema()
//    res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "apply different avg params" in {
    // avg with params:
    //      "type": "agg", "name": "avg", "parameters": {}
    //      "type": "agg", "name": "avg", "parameters": {"null": "drop"}
    //      "type": "agg", "name": "avg", "parameters": {"null": "0"}

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: b=12;c=13, aFeature: 19;18;, pFeature3:31"),
      SourceRow("uid: 42, mFeature: a=21;c=23, aFeature: 29;;27, pFeature2:22, pFeature3:32"),
      SourceRow("uid: 42, mFeature: a=31;b=32, aFeature: ;38;37, pFeature2:23")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

//    df.printSchema()
//    df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> pipelines.avg, "a" -> pipelines.avg_null_0, "b" -> pipelines.avg_null_drop),
      "bar" -> Map("domain" -> pipelines.avg_null_0, "0" -> pipelines.avg, "1" -> pipelines.avg_null_drop),
      "baz" -> Map("domain" -> pipelines.avg_null_0, "baz_f2" -> pipelines.avg_null_drop, "baz_f3" -> pipelines.avg)
    )
    val expected = Seq("[42,Map(b -> 22.0, a -> 17.333334, c -> 18.0),WrappedArray(24.0, 28.0, 21.333334),0.0,22.5,31.5]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

//    res.printSchema()
//    res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "apply different most_freq params" in {
    //      "type": "agg", "name": "most_freq", "parameters": {}
    //      "type": "agg", "name": "most_freq", "parameters": {"rnd_value": "0"}

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: b=12;c=13, aFeature: 19;18;, pFeature3:31"),
      SourceRow("uid: 42, mFeature: a=21;c=13, aFeature: 29;28;27, pFeature2:22, pFeature3:31"),
      SourceRow("uid: 42, mFeature: a=31;b=32;c=33, aFeature: ;18;37, pFeature2:23")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_f1; " +
        "cast(pFeature2 as float) baz_f2; " +
        "cast(pFeature3 as float) baz_f3",
      ";"
    )

//    df.printSchema()
//    df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> pipelines.most_freq, "a" -> pipelines.most_freq_0, "b" -> pipelines.most_freq_0),
      "bar" -> Map("domain" -> pipelines.most_freq_0, "1" -> pipelines.most_freq),
      "baz" -> Map("domain" -> pipelines.most_freq_0, "baz_f3" -> pipelines.most_freq)
    )
    val expected = Seq("[42,Map(b -> 12.0, a -> 21.0, c -> 13.0),WrappedArray(19.0, 18.0, 27.0),null,22.0,31.0]")

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(_.toString)

//    res.printSchema()
//    res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  it should "apply select random most_freq value" in {

    val df: DataFrame = Seq(
      SourceRow("uid: 42, mFeature: a=20;b=12;c=13"),
      SourceRow("uid: 42, mFeature: a=21;b=12;c=13"),
      SourceRow("uid: 42, mFeature: a=22;b=13;c=14"),
      SourceRow("uid: 42, mFeature: a=21;b=13;c=15"),
      SourceRow("uid: 42, mFeature: a=22;b=32;c=33")
    ).toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo",
      ";"
    )

//    df.printSchema()
//    df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> pipelines.most_freq)
    )

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.head.getMap[String, Float](1)

//    res.printSchema()
    res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    assert(Set(21f, 22f).contains(actual("a")))
    assert(Set(12f, 13f).contains(actual("b")))
    assert(13f === actual("c"))
  }

  it should "produce reference values" in {
    // testOnly *EtlFeaturesAgg* -- -z "produce reference values"

    val df = makeDf(List(
      ("42", af(1.791759469, 1.01076878)),
      ("42", af(1.791759469, 0.96028579)),
      ("42", af(0.69314718, 0.92030344)),
      ("42", af(1.6094379, 1.0603224))
    ), float).selectExpr("uid", "arr_float_domain as bar")

    //df.printSchema()
    //df.show(truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "bar" -> Map("domain" -> pipelines.avg, "0" -> pipelines.max)
    )
    val expected = Seq("[42,WrappedArray(1.7917595, 0.9879201)]")

    val res = EtlFeatures.aggregateDomains(df, cfg)
    val actual = res.collect.map(_.toString)

    //res.printSchema()
    //res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    actual should contain theSameElementsAs expected
  }

  private def makeDf(rows: List[(String, Option[Array[Option[Float]]])], ev: Float): DataFrame = {
    import spark.implicits._
    rows.toDF("uid", "arr_float_domain")
  }

  private def af(xs: Any*): Option[Array[Option[Float]]] =
    Some(
      (xs.map {
        case None => None
        case x: Int => Some(x.toFloat)
        case x: Double => Some(x.toFloat)
        case x => Some(x.asInstanceOf[Float])
      }).toArray[Option[Float]]
    )

  private def float: Float = 0

}

class JoinerAppAggregationConcurrentTest extends AnyFlatSpec with should.Matchers  with LocalSpark {

  import EtlFeaturesFunctionsTest._
  import JoinerAppAggregationTest._
  import scala.math.random
  import org.apache.spark.sql
  import spark.implicits._
  import joiner._
  import joiner.implicits._

  it should "not use shared accumulators" in {

    def generateSeq(uid: String, num: Int): Seq[SourceRow] = {
      for (n <- 0 to num) yield
        SourceRow(s"uid: ${uid}, mFeature: a=${random * 20};b=${random * 17};c=${random * 13}, aFeature: ${random};$random;$random, pFeature2: $random, pFeature3: ${random * 10}")
    }

    val df: DataFrame = (generateSeq("42", 1373) ++ generateSeq("33", 659) ++ generateSeq("77", 733))
      .toDF.selectCSVcols(
      "cast(uid as string) uid; " +
        "cast(mFeature as map<string, float>) foo; " +
        "cast(aFeature as array<float>) bar; " +
        "cast(pFeature1 as float) baz_1; " +
        "cast(pFeature2 as float) baz_2; " +
        "cast(pFeature3 as float) baz_3",
      ";"
    ).repartition(6, sql.functions.col("uid"))

    df.printSchema()
    df.show(20, truncate = false)

    val cfg: Map[String, DomainAggregationConfig] = Map(
      "foo" -> Map("domain" -> pipelines.most_freq),
      "bar" -> Map("domain" -> pipelines.avg),
      "baz" -> Map("domain" -> pipelines.sum)
    )

    val res = EtlFeatures.aggregateDomains(df, cfg).cache
    val actual = res.collect.map(row => row.getMap[String, Float](1))

    res.printSchema()
    res.show(truncate = false)

    cols(res) should contain theSameElementsInOrderAs cols(df)
    assert(actual.length === 3)
  }

}

object JoinerAppAggregationTest {

  import joiner.config._

  val avgAggregator = AggregationConfig(
    pipeline = List("filter", "avg"),
    stages = Map(
      "filter" -> AggregationStageConfig(name = "drop_null", kind = "filter",  parameters = Map.empty),
      "avg" ->    AggregationStageConfig(name = "avg",       kind = "agg",     parameters = Map("cast" -> "float"))
    ))

  val minAggregator = pipelines.min
  val maxAggregator = pipelines.max

  object pipelines {

    val avg = AggregationConfig(
      pipeline = List("avg"),
      stages = Map(
        "avg" -> AggregationStageConfig(name = "avg", kind = "agg", parameters = Map.empty)
      ))

    val avg_null_drop = AggregationConfig(
      pipeline = List("avg"),
      stages = Map(
        "avg" -> AggregationStageConfig(name = "avg", kind = "agg", parameters = Map("null" -> "drop"))
      ))

    val avg_null_0 = AggregationConfig(
      pipeline = List("avg"),
      stages = Map(
        "avg" -> AggregationStageConfig(name = "avg", kind = "agg", parameters = Map("null" -> "0"))
      ))

    val most_freq = AggregationConfig(
      pipeline = List("mfreq"),
      stages = Map(
        "mfreq" -> AggregationStageConfig(name = "most_freq", kind = "agg", parameters = Map.empty)
      ))

    val most_freq_0 = AggregationConfig(
      pipeline = List("mfreq"),
      stages = Map(
        "mfreq" -> AggregationStageConfig(name = "most_freq", kind = "agg", parameters = Map("rnd_value" -> "0"))
      ))

    val sum = AggregationConfig(
      pipeline = List("sum"),
      stages = Map(
        "sum" -> AggregationStageConfig(name = "sum", kind = "agg", parameters = Map.empty)
      ))

    val min = AggregationConfig(
      pipeline = List("min"),
      stages = Map(
        "min" -> AggregationStageConfig(name = "min", kind = "agg", parameters = Map.empty)
      ))

    val max = AggregationConfig(
      pipeline = List("max"),
      stages = Map(
        "max" -> AggregationStageConfig(name = "max", kind = "agg", parameters = Map.empty)
      ))
  }
}
