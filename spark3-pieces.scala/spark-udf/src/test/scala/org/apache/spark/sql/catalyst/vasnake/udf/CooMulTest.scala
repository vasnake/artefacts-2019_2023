/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.scalatest._
import org.scalatest.flatspec._

class CooMulTest extends AnyFlatSpec with DataFrameHelpers with LocalSpark {
  import ArrayFloatFixture._
  import functions.generic_coomul

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    functions.registerAll(spark, overrideIfExists = true)
    functions.registerAs(
      funcName = "generic_coomul",
      targetName = "generic_coomul",
      spark,
      overrideIfExists = true
    )
  }

  it should "work with different API" in {
    val (a1, a2) = ("1, 2, 3", "4, 5, 6")
    val expectedValue = "4, 10, 18"
    val colName = "coomul"
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
          .select(generic_coomul("a1", "a2").alias(colName))
      ),
      ("SQL API, aliased function", spark.sql(s"select coomul(${params}) as $colName")),
      ("SQL API, native func.name", spark.sql(s"select generic_coomul(${params}) as $colName"))
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
    val (a1, a2) = ("1, 2, 3", "4, 5, 6")
    val expectedValue = sparkElementType match {
      case _: DecimalType => "4.000000000000000000, 10.000000000000000000, 18.000000000000000000"
      case _: FractionalType => "4.0, 10.0, 18.0"
      case _ => "4, 10, 18"
    }
    val colName = "coomul"
    val params =
      s"cast(array(${a1}) as array<$elementType>), cast(array($a2) as array<$elementType>)"

    show(spark.sql(s"select ${params}"), "source")

    val actual =
      spark.sql(s"select generic_coomul(${params}) as ${colName}").persist(StorageLevel.MEMORY_ONLY)

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

  it should "produce vectors product" in {
    assertResult(coomulEval("1,2,3", "1,2,3"), "1.0, 4.0, 9.0")
  }

  it should "produce null item if pair contains null" in {
    assertResult(
      coomulEval(
        "null,  null, 3,    4",
        "null,  2,    null, 4"
      ),
      "null, null, null, 16.0"
    )
  }

  it should "produce null item if pair contains inf" in {
    assertResult(
      coomulEval(
        "+inf,2,    -inf, 4,    +inf, 6",
        "1,   +inf, 3,    -inf, -inf, 6"
      ),
      "null, null, null, null, null, 36.0"
    )
  }

  it should "produce null item if pair contains nan" in {
    assertResult(
      coomulEval(
        "nan, 2,    nan,  null, nan,  inf,  nan,  8",
        "1,   nan,  nan,  nan,  null, nan,  -inf, 8"
      ),
      "null, null, null, null, null, null, null, 64.0"
    )
  }

  it should "produce empty list if args are empty" in {
    assertResult(coomulEval("", ""), "")
  }

  it should "produce null if argument is null" in {
    val array = "array(float(1), float(2), float(3))"

    Seq(
      s"select coomul(cast(null as array<float>), cast(null as array<float>)) as coomul",
      s"select coomul(cast(null as array<float>), cast(${array} as array<float>)) as coomul",
      s"select coomul(cast(${array} as array<float>), cast(null as array<float>)) as coomul"
    ) foreach { expr =>
      withCaching { actual =>
        checkSchema(actual)
        assert(
          actual.select("coomul").collect().map(_.toString().toLowerCase).toList === List(
            s"[null]".toLowerCase
          )
        )

      }(spark.sql(expr))
    }

  }

  it should "produce null if args of different size" in {
    val array = "array(float(1), float(2))"

    Seq(
      s"select coomul(cast(array() as array<float>), ${array}) as coomul",
      s"select coomul(array(float(1)), ${array}) as coomul",
      s"select coomul(${array}, array(float(1))) as coomul",
      s"select coomul(${array}, cast(array() as array<float>)) as coomul"
    ) foreach { expr =>
      val actual = spark.sql(expr).persist(StorageLevel.MEMORY_ONLY)
      checkSchema(actual)
      assert(
        actual.select("coomul").collect().map(_.toString().toLowerCase).toList === List(
          s"[null]".toLowerCase
        )
      )
      actual.unpersist()
    }
  }

  it should "produce reference values" in {
    // (id, vector1, vector2, expected)
    val data: DataFrame = df(
      List(
        // happy path
        ("1", af(1, 2, 3), af(1, 2, 3), af(1, 4, 9)),

        // item is null
        ("2", af(None, None, 3, 4), af(None, 2, None, 4), af(None, None, None, 16)),

        // item is inf
        (
          "3",
          af(PINF, 2, NINF, 4, PINF, 6),
          af(1, PINF, 3, NINF, NINF, 6),
          af(None, None, None, None, None, 36)
        ),

        // item is nan
        (
          "4",
          af(NAN, 2, NAN, None, NAN, PINF, NAN, 8),
          af(1, NAN, NAN, NAN, None, NAN, NINF, 8),
          af(None, None, None, None, None, None, None, 64)
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
        "coomul(cast(va as array<double>), cast(vb as array<double>)) as coomul",
        "expected"
      )
      .selectExpr("uid", "cast(coomul as array<float>) as coomul", "expected")

    actual.explain(extended = true)
    import org.apache.spark.sql.execution.debug._
    actual.debugCodegen()
    actual.debug()

    checkSchema(actual)

    assert(
      actual.select("coomul").collect().map(_.toString().toLowerCase).toList
        ===
          actual.select("expected").collect().map(_.toString().toLowerCase).toList
    )

    actual.unpersist()
    data.unpersist()
  }

  private def coomulEval(va: String, vb: String): DataFrame = {
    show(spark.sql(s"select ${args(va, vb)}"), "source")
    spark.sql(s"select generic_coomul(${args(va, vb)}) as coomul")
  }

  private def checkSchema(actual: DataFrame): Assertion =
    _assertResult(
      actual,
      "",
      "coomul",
      ArrayType(FloatType, containsNull = true),
      checkSchema = true,
      checkData = false
    )

  private def assertResult(actual: DataFrame, expected: String): Assertion =
    _assertResult(
      actual,
      expected,
      "coomul",
      ArrayType(FloatType, containsNull = true),
      checkSchema = true,
      checkData = true
    )

  private def _assertResult(
    actual: DataFrame,
    expected: String,
    colName: String,
    expectedType: DataType,
    checkSchema: Boolean,
    checkData: Boolean
  ): Assertion =
    withCaching { df =>
      show(df, "result")
      if (checkSchema) assert(df.schema(colName).dataType.sameType(expectedType))
      if (checkData)
        assert(
          df.select(colName).collect().map(_.toString().toLowerCase).toList ===
            List(s"[WrappedArray(${expected})]".toLowerCase)
        )
      assert(checkSchema || checkData)
    }(actual)

  private def args(va: String, vb: String): String =
    s"cast(array(${items(va)}) as array<float>), cast(array(${items(vb)}) as array<float>)"

  private def items(vec: String): String =
    if (vec.isEmpty) vec
    else
      vec
        .split(",")
        .map(_.trim)
        .map(x =>
          if (x.toLowerCase.contains("-inf")) s"float('-Infinity')"
          else if (x.toLowerCase.contains("inf")) s"float('+Infinity')"
          else if (x.toLowerCase.contains("nan")) s"float('NaN')"
          else s"float($x)"
        )
        .mkString(",")
}
