/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql.DataFrame

class ArrayByteTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr(
        "part",
        "uid",
        "transform(feature_list, _x -> if(_x in (null, 'NaN', 'Infinity', '-Infinity'), null, _x)) as feature_list"
      )
      .selectExpr("part", "uid", "cast(feature_list as array<byte>) as feature")
      .orderBy("uid")
      .repartition(4)
  )

  val resultColumnType: String = "ArrayType(ByteType,true)"

  it should "produce null if all collections are null, part E" in {
    val input = inputDF.where("part = 'E'")
    show(input, message = "input")
    sumAndCheck(input, "null")
    minAndCheck(input, "null")
    maxAndCheck(input, "null")
    avgAndCheck(input, "null")
  }

  it should "produce empty collection if all collections are null or empty, part F" in {
    val input = inputDF.where("part = 'F'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray()")
    minAndCheck(input, "WrappedArray()")
    maxAndCheck(input, "WrappedArray()")
    avgAndCheck(input, "WrappedArray()")
  }

  it should "produce null if all values are invalid, part G" in {
    val input = inputDF.where("part = 'G'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray(7, null)")
    minAndCheck(input, "WrappedArray(3, null)")
    maxAndCheck(input, "WrappedArray(4, null)")
    avgAndCheck(input, "WrappedArray(3, null)")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray(7, 5)")
    minAndCheck(input, "WrappedArray(3, 2)")
    maxAndCheck(input, "WrappedArray(4, 3)")
    avgAndCheck(input, "WrappedArray(3, 2)")
  }

  it should "consider absent values as null, part J" in {
    val input = inputDF.where("part = 'J'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray(1, 2, 3)")
    minAndCheck(input, "WrappedArray(1, 2, 3)")
    maxAndCheck(input, "WrappedArray(1, 2, 3)")
    avgAndCheck(input, "WrappedArray(1, 2, 3)")
  }

}
