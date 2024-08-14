/**
 * Created by vasnake@gmail.com on 2024-08-14
 */
package com.github.vasnake.spark.app.datasets

import com.github.vasnake.spark.test.SimpleLocalSpark
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec._
import org.scalatest.matchers._

class CompareDatasetsAppTest extends AnyFlatSpec with should.Matchers with SimpleLocalSpark {

  private val ops = new CompareDatasetsApp.CopyPasteToSparkShell.DataFrameActions {
    override def cache(df: DataFrame): DataFrame = df
    override def checkpoint(df: DataFrame): DataFrame = df
  }

  private val int: Int = 0
  private val float: Float = 0
  private val fmap: Map[String, Float] = Map.empty
  private val imap: Map[String, Int] = Map.empty

  it should "find no diff in array<int> domain" in {

    val a: DataFrame = df(List(
      ("uid_1", ai(1, 2)),
      ("uid_2", None),
      ("uid_3", ai()),
      ("uid_4", ai(None, None)),
      ("uid_5", ai(1, None)),
      ("uid_6", ai(None, 3, None)),
      ("uid_7", ai(1, None))
    ), int)

    val b: DataFrame = df(List(
      ("uid_1", ai(1, 2)),
      ("uid_2", None),
      ("uid_3", ai()),
      ("uid_4", ai(None, None)),
      ("uid_5", ai(1, None)),
      ("uid_6", ai(None, 3, None)),
      ("uid_7", ai(1, None))
    ), int)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)

    diffDF.collect().isEmpty shouldEqual true
  }

  it should "find 7 diff in array<int> domain" in {

    val a: DataFrame = df(List(
      ("uid_1", ai(1, 2)),
      ("uid_2", None),
      ("uid_3", ai()),
      ("uid_4", ai(None, None)),
      ("uid_5", ai(1, 2)),
      ("uid_6", ai(None, 3, None)),
      ("uid_7", ai(1, None, 2))
    ), int)

    val b: DataFrame = df(List(
      ("uid_1", ai(1, 3)),
      ("uid_2", ai()),
      ("uid_3", None),
      ("uid_4", ai(None, 1)),
      ("uid_5", ai(None, 2)),
      ("uid_6", ai(None, None)),
      ("uid_7", ai(1, 2, None))
    ), int)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)
    diffDF.show(truncate = false)

    diffDF.collect().length shouldEqual 7
  }

  it should "find no diff in array<float> domain" in {

    val a: DataFrame = df(List(
      ("uid_1", af(1, 2)),
      ("uid_2", None),
      ("uid_3", af()),
      ("uid_4", af(None, None)),
      ("uid_5", af(1, None)),
      ("uid_6", af(None, 3, None)),
      ("uid_7", af(1, None))
    ), float)

    val b: DataFrame = df(List(
      ("uid_1", af(1, 2)),
      ("uid_2", None),
      ("uid_3", af()),
      ("uid_4", af(None, None)),
      ("uid_5", af(1, None)),
      ("uid_6", af(None, 3, None)),
      ("uid_7", af(1, None))
    ), float)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)

    diffDF.collect().isEmpty shouldEqual true
  }

  it should "find 7 diff in array<float> domain" in {

    val a: DataFrame = df(List(
      ("uid_0", af(1, 2)),
      ("uid_1", af(1, 2)),
      ("uid_2", None),
      ("uid_3", af()),
      ("uid_4", af(None, None)),
      ("uid_5", af(1, 2)),
      ("uid_6", af(None, 3, None)),
      ("uid_7", af(1, None, 2))
    ), float)

    val b: DataFrame = df(List(
      ("uid_0", af(1, 2.00001)),
      ("uid_1", af(1, 2.0001)),
      ("uid_2", af()),
      ("uid_3", None),
      ("uid_4", af(None, 1)),
      ("uid_5", af(None, 2)),
      ("uid_6", af(None, None)),
      ("uid_7", af(1, 2, None))
    ), float)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)
    diffDF.show(truncate = false)

    diffDF.collect().length shouldEqual 7
  }

  it should "find no diff in map<string,float> domain" in {

    val a: DataFrame = df(List(
      ("uid_1", mf("k1", "k2")(1, 2)),
      ("uid_2", None),
      ("uid_3", mf()()),
      ("uid_4", mf("k1", "k2")(None, None)),
      ("uid_5", mf("k1", "k2")(1, None)),
      ("uid_6", mf("k1", "k2", "k3")(None, 3, None)),
      ("uid_7", mf("k1", "k2")(1, None))
    ), float, fmap)

    val b: DataFrame = df(List(
      ("uid_1", mf("k1", "k2")(1, 2)),
      ("uid_2", None),
      ("uid_3", mf()()),
      ("uid_4", mf("k1", "k2")(None, None)),
      ("uid_5", mf("k1", "k2")(1, None)),
      ("uid_6", mf("k1", "k2", "k3")(None, 3, None)),
      ("uid_7", mf("k1", "k2")(1, None))
    ), float, fmap)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)

    diffDF.collect().isEmpty shouldEqual true
  }

  it should "find 8 diff in map<string,float> domain" in {

    val a: DataFrame = df(List(
      ("uid_0", mf("k1", "k2")(1, 2)),
      ("uid_1", mf("k1", "k2")(1, 2)),
      ("uid_2", None),
      ("uid_3", mf()()),
      ("uid_4", mf("k1", "k2")(None, None)),
      ("uid_5", mf("k1", "k2")(1, 2)),
      ("uid_6", mf("k1", "k2", "k3")(None, 3, None)),
      ("uid_7", mf("k1", "k2", "k3")(1, None, 2)),
      ("uid_8", mf("k1", "k2", "k3")(1, None, 2))
    ), float, fmap)

    val b: DataFrame = df(List(
      ("uid_0", mf("k1", "k2")(1, 2.00001)),
      ("uid_1", mf("k1", "k2")(1, 2.0001)),
      ("uid_2", mf()()),
      ("uid_3", None),
      ("uid_4", mf("k1", "k2")(None, 1)),
      ("uid_5", mf("k1", "k2")(None, 2)),
      ("uid_6", mf("k1", "k2")(None, None)),
      ("uid_7", mf("k1", "k2", "k3")(1, 2, None)),
      ("uid_8", mf("k1", "kd", "k3")(None, None, 2.0001))
    ), float, fmap)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)
    diffDF.show(truncate = false)

    diffDF.collect().length shouldEqual 8
  }

  it should "find no diff in map<string,int> domain" in {

    val a: DataFrame = df(List(
      ("uid_1", mi("k1", "k2")(1, 2)),
      ("uid_2", None),
      ("uid_3", mi()()),
      ("uid_4", mi("k1", "k2")(None, None)),
      ("uid_5", mi("k1", "k2")(1, None)),
      ("uid_6", mi("k1", "k2", "k3")(None, 3, None)),
      ("uid_7", mi("k1", "k2")(1, None))
    ), int, imap)

    val b: DataFrame = df(List(
      ("uid_1", mi("k1", "k2")(1, 2)),
      ("uid_2", None),
      ("uid_3", mi()()),
      ("uid_4", mi("k1", "k2")(None, None)),
      ("uid_5", mi("k1", "k2")(1, None)),
      ("uid_6", mi("k1", "k2", "k3")(None, 3, None)),
      ("uid_7", mi("k1", "k2")(1, None))
    ), int, imap)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)

    diffDF.collect().isEmpty shouldEqual true
  }

  it should "find 8 diff in map<string,int> domain" in {

    val a: DataFrame = df(List(
      ("uid_1", mi("k1", "k2")(1, 3)),
      ("uid_2", None),
      ("uid_3", mi()()),
      ("uid_4", mi("k1", "k2")(None, None)),
      ("uid_5", mi("k1", "k2")(1, 2)),
      ("uid_6", mi("k1", "k2", "k3")(None, 3, None)),
      ("uid_7", mi("k1", "k2", "k3")(1, None, 2)),
      ("uid_8", mi("k1", "k2", "k3")(1, None, 2))
    ), int, imap)

    val b: DataFrame = df(List(
      ("uid_1", mi("k1", "k2")(1, 2)),
      ("uid_2", mi()()),
      ("uid_3", None),
      ("uid_4", mi("k1", "k2")(None, 1)),
      ("uid_5", mi("k1", "k2")(None, 2)),
      ("uid_6", mi("k1", "k2")(None, None)),
      ("uid_7", mi("k1", "k2", "k3")(1, 2, None)),
      ("uid_8", mi("k1", "kd", "k3")(None, None, 3))
    ), int, imap)

    val diffDF = CompareDatasetsApp.CopyPasteToSparkShell.Functions.comparePartitions(a, b, ops)
    diffDF.show(truncate = false)

    diffDF.collect().length shouldEqual 8
  }

  it should "compare float with some epsilon" in {
    import CompareDatasetsApp.LowLevelFunctions.compareDouble

    compareDouble(0.0, 0.0) shouldEqual 0
    compareDouble(-0.0, 0.0) shouldEqual 0
    compareDouble(-0.0, -0.0) shouldEqual 0
    compareDouble(0.0, -0.0) shouldEqual 0

    compareDouble(123.456, 123.456) shouldEqual 0
    compareDouble(123.456, 123.454) shouldEqual 1
    compareDouble(123.455, 123.457) shouldEqual -1

    compareDouble(0.00001, 0.00001) shouldEqual 0
    compareDouble(0.000001, 0.000002) shouldEqual 0
    compareDouble(0.000002, 0.000001) shouldEqual 0

    compareDouble(-0.000001, 0.000001) shouldEqual 0
    compareDouble(0.000001, -0.000001) shouldEqual 0
    compareDouble(-0.000001, -0.000001) shouldEqual 0
    compareDouble(0.000001, 0.000001) shouldEqual 0

    compareDouble(0.00001, 0.00002) shouldEqual 0
    compareDouble(-0.00001, -0.00002) shouldEqual 0

    compareDouble(-0.00001, 0.00002) shouldEqual -1
    compareDouble(0.00001, -0.00002) shouldEqual 1

    compareDouble(0.000019, 0.00003) shouldEqual -1
    compareDouble(0.00003, 0.000019) shouldEqual 1

    compareDouble(0.00002, 0.00001, epsilon = 0.000009) shouldEqual 1
    compareDouble(0.00001, -0.00002, epsilon = 0.000009) shouldEqual 1
    compareDouble(0.00001, 0.00002, epsilon = 0.000009) shouldEqual -1
    compareDouble(-0.00002, 0.00001, epsilon = 0.000009) shouldEqual -1

    compareDouble(28007.932, 28007.934) shouldEqual 0
    compareDouble(28007.934, 28007.932) shouldEqual 0
    compareDouble(28007.94, 28007.93) shouldEqual 0
    compareDouble(28007.9, 28007.8) shouldEqual 0
    compareDouble(-28007.934, -28007.932) shouldEqual 0

    compareDouble(28007.9, 28007.7) shouldEqual 0
    compareDouble(28007.9, 28007.6) shouldEqual 1
    compareDouble(28007.6, 28007.9) shouldEqual -1

    compareDouble(28007.934, -28007.932) shouldEqual 1
    compareDouble(-28007.934, 28007.932) shouldEqual -1

    compareDouble(Double.NaN, Double.NaN) shouldEqual 0
    compareDouble(Double.NegativeInfinity, Double.NegativeInfinity) shouldEqual 0
    compareDouble(Double.PositiveInfinity, Double.PositiveInfinity) shouldEqual 0

    compareDouble(Double.NaN, Double.MaxValue) shouldEqual 1
    compareDouble(Double.MaxValue, Double.NaN) shouldEqual -1

    compareDouble(Double.PositiveInfinity, Double.NegativeInfinity) shouldEqual 1
    compareDouble(Double.NegativeInfinity, Double.PositiveInfinity) shouldEqual -1

    compareDouble(Double.PositiveInfinity, Double.MaxValue) shouldEqual 1
    compareDouble(Double.MaxValue, Double.PositiveInfinity) shouldEqual -1

    compareDouble(Double.NegativeInfinity, Double.MinValue) shouldEqual -1
    compareDouble(Double.MinValue, Double.NegativeInfinity) shouldEqual 1

    compareDouble(Double.NaN, Double.PositiveInfinity) shouldEqual 1
    compareDouble(Double.NegativeInfinity, Double.NaN) shouldEqual -1

  }

  private def mf(keys: String*)(xs: Any*): Option[Map[String, Option[Float]]] = {
    af(xs: _*)
      .map(
        xs =>
          keys.zip(xs).toMap
      )
  }

  private def mi(keys: String*)(xs: Any*): Option[Map[String, Option[Int]]] = {
    ai(xs: _*)
      .map(
        xs =>
          keys.zip(xs).toMap
      )
  }

  private def ai(xs: Any*): Option[Array[Option[Int]]] =
    Some(
      (xs.map {
        case None => None
        case x => Some(x.asInstanceOf[Int])
      }).toArray[Option[Int]]
    )

  private def af(xs: Any*): Option[Array[Option[Float]]] =
    Some(
      (xs.map {
        case None => None
        case x: Int => Some(x.toFloat)
        case x: Double => Some(x.toFloat)
        case x => Some(x.asInstanceOf[Float])
      }).toArray[Option[Float]]
    )

  // N.B. not working due to type erasure, use ClassTag:
  //private def make_array[T](xs: Any*): Option[Array[Option[T]]] =
  //  Some(
  //    (xs.map {
  //      case None => None
  //      case x: T => Some(x.asInstanceOf[T])
  //    }).toArray[Option[T]]
  //  )

  private def df(rows: List[(String, Option[Array[Option[Int]]])], ev: Int): DataFrame = {
    // TODO: extended createDF function https://github.com/MrPowers/spark-daria/blob/0898d1f7e81b847ddd27797f5590930822aaea67/src/main/scala/com/github/mrpowers/spark/daria/sql/SparkSessionExt.scala#L58
    import spark.implicits._
    rows.toDF("uid", "arr_int_domain")
  }

  private def df(rows: List[(String, Option[Array[Option[Float]]])], ev: Float): DataFrame = {
    import spark.implicits._
    rows.toDF("uid", "arr_float_domain")
  }

  private def df(rows: List[(String, Option[Map[String, Option[Int]]])], ev: Int, coll: Map[String, Int]): DataFrame = {
    import spark.implicits._
    rows.toDF("uid", "map_int_domain")
  }

  private def df(rows: List[(String, Option[Map[String, Option[Float]]])], ev: Float, coll: Map[String, Float]): DataFrame = {
    import spark.implicits._
    rows.toDF("uid", "map_float_domain")
  }

}
