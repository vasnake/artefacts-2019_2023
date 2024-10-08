/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test._
import org.apache.spark.sql
import org.apache.spark.storage.StorageLevel
//import org.scalatest._
import org.scalatest.flatspec._

class SemiDiffTest extends AnyFlatSpec with DataFrameHelpers with LocalSpark {
  import sql.DataFrame
  import sql.types._
  import ArrayFloatFixture._
  import functions.generic_semidiff

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    functions.registerAll(spark, overrideIfExists = true)
    functions.registerAs(
      funcName = "generic_semidiff",
      targetName = "generic_semidiff",
      spark,
      overrideIfExists = true
    )
  }

  it should "work with different API" in {
    val (a1, a2) = ("18, 22, 37", "10, 2, 1")
    val expectedValue = "4, 10, 18"
    val colName = "semidiff"
    val expectedType = ArrayType(IntegerType, containsNull = true)
    val elementType = "int"
    val params =
      s"cast(array(${a1}) as array<$elementType>), cast(array($a2) as array<$elementType>)"

    List(
      (
        "Dataset API",
        spark
          .sql(
            s"select cast(array(${a1}) as array<$elementType>) as a1, cast(array(${a2}) as array<$elementType>) as a2"
          )
          .select(generic_semidiff("a1", "a2").alias(colName))
      ),
      ("SQL API, aliased function", spark.sql(s"select semidiff(${params}) as $colName")),
      ("SQL API, native func.name", spark.sql(s"select generic_semidiff(${params}) as $colName"))
    ).foreach {
      case (msg, df) =>
        show(df, msg)
        assert(df.schema(colName).dataType.sameType(expectedType))
        assert(
          df.select(colName).collect().map(_.toString().toLowerCase).toList === List(
            s"[WrappedArray(${expectedValue})]".toLowerCase
          )
        )
    }
  }

  private def checkType(elementType: String, sparkElementType: DataType) = {
    //      ByteType,
    //      ShortType,
    //      IntegerType,
    //      LongType,
    //      FloatType,
    //      DoubleType
    //      DecimalType

    val expectedType = ArrayType(sparkElementType, containsNull = true)
    val (a1, a2) = ("18, 22, 37", "10, 2, 1")
    val expectedValue = sparkElementType match {
      case _: DecimalType => "4.000000000000000000, 10.000000000000000000, 18.000000000000000000"
      case _: FractionalType => "4.0, 10.0, 18.0"
      case _ => "4, 10, 18"
    }
    val colName = "semidiff"
    val params =
      s"cast(array(${a1}) as array<$elementType>), cast(array($a2) as array<$elementType>)"

    show(spark.sql(s"select ${params}"), "source")

    val actual = spark
      .sql(s"select generic_semidiff(${params}) as ${colName}")
      .persist(StorageLevel.MEMORY_ONLY)

    show(actual, "target")

    assert(actual.schema(colName).dataType.sameType(expectedType))
    assert(
      actual.select(colName).collect().map(_.toString().toLowerCase).toList === List(
        s"[WrappedArray(${expectedValue})]".toLowerCase
      )
    )

    actual.unpersist()
  }

  it should "work with byte data type" in {
    checkType("byte", ByteType)
  }

  it should "work with short data type" in {
    checkType("short", ShortType)
  }

  it should "work with integer data type" in {
    checkType("int", IntegerType)
  }

  it should "work with long data type" in {
    checkType("long", LongType)
  }

  it should "work with float data type" in {
    checkType("float", FloatType)
  }

  it should "work with double data type" in {
    checkType("double", DoubleType)
  }

  it should "work with decimal data type" in {
    checkType("decimal(38,18)", DecimalType.SYSTEM_DEFAULT)
  }

  it should "produce reference values" in {

    // (id, vector1, vector2, expected)
    val data: DataFrame = df(
      List(
        // happy path
        ("1.1", af(1, 2, 3), af(1, 1, 1), af(0, 0.5, 1)),
        ("1.2", af(1, 1, 1), af(1, 2, 3), af(0, -0.5, -1)),

        // item is null
        ("2", af(None, None, 3, 4), af(None, 2, None, 44), af(None, None, None, -20)),

        // item is inf
        (
          "3",
          af(PINF, 2, NINF, 4, PINF, 6),
          af(1, PINF, 3, NINF, NINF, 66),
          af(None, None, None, None, None, -30)
        ),

        // item is nan
        (
          "4",
          af(NAN, 2, NAN, None, NAN, PINF, NAN, 8),
          af(1, NAN, NAN, NAN, None, NAN, NINF, 88),
          af(None, None, None, None, None, None, None, -40)
        ),

        // both arguments are empty
        ("5", af(), af(), af()),

        // argument is null
        ("6", None, None, None),
        ("7", None, af(1, 2), None),
        ("8", af(1, 2), None, None),

        // different size
        ("9", af(1, 2), af(1, 2, 3), None),
        ("10", af(1, 2, 3), af(1, 2), None)
      )
    )
      .persist(StorageLevel.DISK_ONLY) // N.B. codegen ON switch here

    val actual = data
      .selectExpr(
        "upper(uid) as uid",
        "semidiff(cast(va as array<double>), cast(vb as array<double>)) as semidiff",
        "expected"
      )
      .selectExpr("uid", "cast(semidiff as array<float>) as semidiff", "expected")

    actual.explain(extended = true)
    import org.apache.spark.sql.execution.debug._
    actual.debugCodegen()
    actual.debug()

    show(actual, "result")

    assert(
      actual
        .schema("semidiff")
        .toString() === "StructField(semidiff,ArrayType(FloatType,true),true)"
    )

    assert(
      actual.select("semidiff").collect().map(_.toString().toLowerCase).toList
        ===
          actual.select("expected").collect().map(_.toString().toLowerCase).toList
    )

    data.unpersist()
  }
}
