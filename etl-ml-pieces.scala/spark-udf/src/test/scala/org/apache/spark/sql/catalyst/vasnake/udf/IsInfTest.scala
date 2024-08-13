/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
//import org.scalatest._
import org.scalatest.flatspec._

class IsInfTest extends AnyFlatSpec with DataFrameHelpers with LocalSpark {
  import functions.generic_isinf

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    functions.registerAll(spark, overrideIfExists = true)
    functions.registerAs(
      funcName = "generic_isinf",
      targetName = "generic_isinf",
      spark,
      overrideIfExists = true,
    )
  }

  it should "work with different API" in {
    val colName = "isinf"
    val expectedValue = "true"
    val expectedType = BooleanType

    List(
      (
        "Dataset API",
        spark
          .sql(s"select float('Infinity') as x")
          .select(generic_isinf("x").alias(colName)),
      ),
      ("SQL API, aliased function", spark.sql(s"select isinf(double('-Infinity')) as $colName")),
      (
        "SQL API, native func.name",
        spark.sql(s"select generic_isinf(float('-Infinity')) as $colName"),
      ),
    ).foreach {
      case (msg, df) =>
        show(df, msg)
        assert(df.schema(colName).dataType.sameType(expectedType))
        assert(
          df.select(colName).collect().map(_.toString().toLowerCase).toList === List(
            s"[${expectedValue}]".toLowerCase
          )
        )
    }
  }

  private def checkType(
    elementType: String,
    value: String = "0",
    expected: String = "false",
  ) = {
    val params = s"cast($value as ${elementType})"

    show(spark.sql(s"select $params"), "source")

    val actual = spark.sql(s"select generic_isinf($params) as x").persist(StorageLevel.MEMORY_ONLY)

    show(actual, "target")

    assert(actual.schema("x").dataType.sameType(BooleanType))
    assert(
      actual.select("x").collect().map(_.toString().toLowerCase).toList === List(
        s"[$expected]".toLowerCase
      )
    )

    actual.unpersist()
  }

  it should "work with byte data type" in {
    checkType("byte")
  }

  it should "work with short data type" in {
    checkType("short")
  }

  it should "work with int data type" in {
    checkType("int")
  }

  it should "work with long data type" in {
    checkType("long")
  }

  it should "work with float data type" in {
    checkType("float")
    checkType("float", value = "42", expected = "false")
    checkType("float", value = "'Infinity'", expected = "true")
    checkType("float", value = "'-Infinity'", expected = "true")
  }

  it should "work with double data type" in {
    checkType("double")
    checkType("double", value = "42", expected = "false")
    checkType("double", value = "'Infinity'", expected = "true")
    checkType("double", value = "'-Infinity'", expected = "true")
  }

  it should "work with decimal data type" in {
    checkType("decimal")
  }

  it should "work with string data type" in {
    checkType("string", value = "'Infinity'", expected = "true")
    checkType("string", value = "'42'", expected = "false")
    checkType("string", value = "'some text'", expected = "false") // cast to double, produce null, null is not inf

  }

  it should "work in composed expression" in {
    import spark.implicits._
    val df = List(
      ("a", Some(3.14)),
      ("b", None),
      ("c", Some(Double.NaN)),
      ("d", Some(Double.NegativeInfinity)),
      ("e", Some(Double.PositiveInfinity)),
    ).toDF("uid", "x")

    show(df, "source")

    val actual = df
      .selectExpr("upper(uid) as uid", "cast(x as float) as x")
      .where("not isnull(x) and not isnan(x) and not isinf(x)")
      .persist(StorageLevel.MEMORY_ONLY)

    show(actual, "target")

    assert(actual.schema("x").dataType.sameType(FloatType))
    assert(actual.select("x").collect().map(_.toString().toLowerCase).toList === List("[3.14]"))

    actual.unpersist()
  }

  it should "work with GenCode logic" in {
    import spark.implicits._

    val df = List(
      ("a", Some(3.14f)),
      ("b", None),
      ("c", Some(Float.NaN)),
      ("d", Some(Float.NegativeInfinity)),
      ("e", Some(Float.PositiveInfinity)),
    )
      .toDF("uid", "x")
      .persist(StorageLevel.DISK_ONLY) // N.B. codegen ON switch here

    val actual = df
      .where("uid != 'f'")
      .selectExpr(
        "upper(uid) as uid",
        "cast(x as float) as x",
        "isinf(x)",
        "isinf(float(x))",
        "isinf(double(x))",
        "isinf(string(x))",
      )
      .where("uid != 'g'")
      .where("isinf(x)")

    actual.explain(extended = true)

    import org.apache.spark.sql.execution.debug._
    actual.debugCodegen()

    // actions
    actual.debug()
    show(actual, "target")
    assert(actual.schema("x").dataType.sameType(FloatType))
    assert(
      actual.orderBy("uid").select("x").collect().map(_.toString().toLowerCase).toList === List(
        "[-infinity]",
        "[infinity]",
      )
    )

    df.unpersist()
  }
}
