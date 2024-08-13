/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
//import matchers._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.github.vasnake.spark.test.{DataFrameHelpers, LocalSpark}

class MatMulTest extends AnyFlatSpec with DataFrameHelpers with LocalSpark {

  import ArrayFloatFixture._
  import functions.generic_matmul

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    functions.registerAll(spark, overrideIfExists = true)
    functions.registerAs(funcName = "generic_matmul", targetName = "generic_matmul", spark, overrideIfExists = true)
  }

  it should "work with different API" in {
    val (a1, a2) = ("1, 2, 3, 4", "5, 6")
    val expectedValue = "17, 39"
    val colName = "matmul"
    val expectedType = ArrayType(IntegerType, containsNull = true)
    val elementType = "int"
    val params = s"cast(array(${a1}) as array<$elementType>), cast(array($a2) as array<$elementType>)"

    List(
      ("Dataset API", spark.sql(s"select cast(array(${a1}) as array<$elementType>) as a1, cast(array(${a2}) as array<$elementType>) as a2")
        .select(generic_matmul("a1", "a2").alias(colName))),
      ("SQL API, aliased function", spark.sql(s"select matmul(${params}) as $colName")),
      ("SQL API, native func.name", spark.sql(s"select generic_matmul(${params}) as $colName"))
    ).foreach { case (msg, df) => {
      show(df, msg)
      assert(df.schema(colName).dataType.sameType(expectedType))
      assert(df.select(colName).collect().map(_.toString().toLowerCase).toList === List(s"[WrappedArray(${expectedValue})]".toLowerCase))
    }}
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
    val (a1, a2) = ("1, 2, 3, 4", "5, 6")
    val expectedValue = sparkElementType match {
      case _: DecimalType => "17.000000000000000000, 39.000000000000000000"
      case _: FractionalType => "17.0, 39.0"
      case _ => "17, 39"
    }
    val colName = "matmul"
    val params = s"cast(array(${a1}) as array<$elementType>), cast(array($a2) as array<$elementType>)"

    show(spark.sql(s"select ${params}"), "source")

    val actual = spark.sql(s"select generic_matmul(${params}) as ${colName}").persist(StorageLevel.MEMORY_ONLY)

    show(actual, "target")

    assert(actual.schema(colName).dataType.sameType(expectedType))
    assert(actual.select(colName).collect().map(_.toString().toLowerCase).toList === List(s"[WrappedArray(${expectedValue})]".toLowerCase))

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
    val data: DataFrame = df(List(
      // happy path
      ("1.1", af(1, 2, 3, 4), af(5, 6), af(17, 39)), // mat * vec
      ("1.2", af(1, 2, 3, 4), af(5, 6, 7, 8), af(19, 22, 43, 50)), // mat * mat

      // item is invalid, mat * vec
      ("2.1", af(1, 2, None, 4), af(5, 6), af(17, None)),
      ("2.2", af(1, 2, NINF, 4), af(5, 6), af(17, None)),
      ("2.3", af(1, 2, NAN, 4), af(5, 6), af(17, None)),
      ("2.4", af(1, 2, 3, 4), af(None, 6), af(None, None)),
      ("2.5", af(1, 2, 3, 4), af(PINF, 6), af(None, None)),
      ("2.6", af(1, 2, 3, 4), af(NAN, 6), af(None, None)),
      ("2.7", af(1, 2, None, 4), af(NAN, 6), af(None, None)),

      // item is invalid, mat * mat
      ("3.1", af(1, None, 3, 4), af(5, 6, 7, 8), af(None, None, 43, 50)),
      ("3.2", af(1, NINF, 3, 4), af(5, 6, 7, 8), af(None, None, 43, 50)),
      ("3.3", af(1, NAN, 3, 4), af(5, 6, 7, 8), af(None, None, 43, 50)),
      ("3.4", af(1, 2, 3, 4), af(5, None, 7, 8), af(19, None, 43, None)),
      ("3.5", af(1, 2, 3, 4), af(5, PINF, 7, 8), af(19, None, 43, None)),
      ("3.6", af(1, 2, 3, 4), af(5, NAN, 7, 8), af(19, None, 43, None)),
      ("3.7", af(1, None, 3, 4), af(5, NAN, 7, 8), af(None, None, 43, None)),

      // both arguments are empty
      ("4", af(), af(), af()),

      // argument is null
      ("5.1", None, None, None),
      ("5.2", None, af(1, 2, 3), None),
      ("5.3", af(1, 2, 3), None, None),

      // different size
      ("6.1", af(1, 2, 3), af(4, 5, 6), None),
      ("6.2", af(1, 2, 3, 4), af(5, 6, 7), None)
    ))
      .persist(StorageLevel.DISK_ONLY) // N.B. codegen ON switch here

    val actual = data
      .selectExpr("uid", "matmul(va, vb) as matmul", "expected")

    actual.explain(extended = true)
    import org.apache.spark.sql.execution.debug._
    actual.debugCodegen()
    actual.debug()

    show(actual, "result")

    assert(actual.schema("matmul").toString() === "StructField(matmul,ArrayType(FloatType,true),true)")

    assert(
      actual.select("matmul").collect().map(_.toString().toLowerCase).toList
        ===
        actual.select("expected").collect().map(_.toString().toLowerCase).toList
    )

    data.unpersist()
  }

}
