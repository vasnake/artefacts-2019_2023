/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.spark.test

import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.scalatest._
import org.scalatest.matchers.should

trait DataFrameHelpers extends should.Matchers {

  // sbt> set ThisBuild / Test / envVars := Map("DEBUG_MODE" -> "true")
  lazy val debugMode: Boolean = sys.env.getOrElse("DEBUG_MODE", "false").toBoolean

  def show(
    df: DataFrame,
    message: String = "",
    nrows: Int = 100,
    force: Boolean = false
  ): Unit =
    if (debugMode || force) {
      val _df = df.persist(newLevel = StorageLevel.MEMORY_ONLY)
      val rows = _df.collect()
      if (message.nonEmpty) println(s"\n${message}; rows: ${rows.length}; partitions: ${_df.rdd.getNumPartitions}\n")
      _df.explain(extended = true)
      _df.printSchema()
      _df.show(nrows, truncate = false)
      println(rows.mkString("\n"))
      _df.unpersist()
    }

  def cache(df: DataFrame): DataFrame = df.persist(newLevel = StorageLevel.MEMORY_ONLY)

  def withCaching[T](fun: DataFrame => T): DataFrame => T =
    (df: DataFrame) => {
      val pdf = df.persist(StorageLevel.MEMORY_ONLY)
      val res = fun(pdf)
      pdf.unpersist()
      res
    }

  def withUnpersist[T](fun: DataFrame => T)(df: DataFrame): T = {
    val res = fun(df)
    df.unpersist()
    res
  }

  def compareSchemas(
    actual: StructType,
    expected: StructType,
    checkNull: Boolean = false
  ): Unit = {
    assert(actual.length === expected.length)

    actual.fields.sortBy(_.name).zip(expected.fields.sortBy(_.name)).foreach {
      case (a, e) =>
        assert(a.name === e.name)
        a.dataType match {
          case ArrayType(typ, cn) => // array elements nullable?
            assert(typ === e.dataType.asInstanceOf[ArrayType].elementType)
            if (checkNull) assert(cn === e.dataType.asInstanceOf[ArrayType].containsNull)
          case _ =>
            assert(a.dataType === e.dataType)
        }
        if (checkNull) assert(a.nullable === e.nullable)
    }
  }

  def compareDataframes(
    actual: DataFrame,
    expected: DataFrame,
    checkSchemaNull: Boolean = false,
    accuracy: Int = 7,
    orderby: String = "uid",
    unpersist: Boolean = false
  ): Assertion = {

    compareSchemas(actual.schema, expected.schema, checkSchemaNull)

    assert(actual.count() === expected.count())

    val cols = actual.schema.fieldNames.sorted
    val res = actual
      .selectExpr(cols: _*)
      .orderBy(orderby)
      .collect()
      .zip(
        expected.selectExpr(cols: _*).orderBy(orderby).collect()
      )
      .map {
        case (aRow, eRow) => assert(rowRepr(aRow, accuracy) === rowRepr(eRow, accuracy))
      }
      .headOption
      .getOrElse(Succeeded)

    if (unpersist) actual.unpersist()
    res
  }

  def rowRepr(row: Row, accuracy: Int): Seq[String] = {

    def d2s(x: Any): String =
      if (x == null) "null"
      else s"%1.${accuracy}f".format(x.toString.toDouble)

    (0 until row.length).map { i =>
      if (row.isNullAt(i)) "null"
      else
        row.schema(i).dataType.sql match {
          case "DOUBLE" | "FLOAT" => d2s(row.get(i))
          case "ARRAY<DOUBLE>" =>
            row
              .getAs[mutable.WrappedArray[Any]](i)
              .map(x => d2s(x))
              .toList
              .mkString("Array(", ",", ")")
          case _ => row.get(i).toString
        }
    }.toList
  }

  def compareColumns(
    df: DataFrame,
    actual: String,
    expected: String,
    accuracy: Int = 7,
    orderby: String = "uid"
  )(implicit
    spark: SparkSession
  ): Assertion =
    compareDoubleColumns(df, df, actual, expected, accuracy, orderby)

  def compareDoubleColumns(
    actualDf: DataFrame,
    expectedDf: DataFrame,
    actualCol: String,
    expectedCol: String,
    accuracy: Int = 7,
    orderby: String = "uid"
  )(implicit
    spark: SparkSession
  ): Assertion = {
    import spark.implicits._

    def getSeq(df: DataFrame, name: String): Seq[String] =
      df.orderBy(orderby)
        .selectExpr(s"cast(${name} as double) as ${name}")
        .as[Option[Double]]
        .collect
        .map(_.map(v => s"%1.${accuracy}f".format(v)).getOrElse("None"))
        .toList

    assert(getSeq(actualDf, actualCol) === getSeq(expectedDf, expectedCol))
  }

  def compareArrayDoubleColumns(
    actualDf: DataFrame,
    expectedDf: DataFrame,
    actualCol: String,
    expectedCol: String,
    accuracy: Int = 7,
    orderby: String = "uid"
  )(implicit
    spark: SparkSession
  ): Assertion = {
    import spark.implicits._

    def getSeq(df: DataFrame, name: String): Seq[String] =
      df.orderBy(orderby)
        .selectExpr(s"cast(${name} as array<double>) as ${name}")
        .as[Option[Array[Option[Double]]]]
        .collect
        .map(optArr =>
          optArr
            .map(arr =>
              arr
                .map(optVal => optVal.map(v => s"%1.${accuracy}f".format(v)).getOrElse("None"))
                .mkString("Array(", ",", ")")
            )
            .getOrElse("None")
        )
        .toList

    assert(getSeq(actualDf, actualCol) === getSeq(expectedDf, expectedCol))
  }

  def compareStringColumns(
    actualDf: DataFrame,
    expectedDf: DataFrame,
    actualCol: String,
    expectedCol: String,
    orderby: String = "uid"
  )(implicit
    spark: SparkSession
  ): Assertion = {
    import spark.implicits._

    def getSeq(df: DataFrame, name: String): Seq[String] =
      df.orderBy(orderby)
        .selectExpr(s"cast(${name} as string) as ${name}")
        .as[Option[String]]
        .collect
        .map(x => x.getOrElse("null"))
        .toList

    assert(getSeq(actualDf, actualCol) === getSeq(expectedDf, expectedCol))
  }
}
