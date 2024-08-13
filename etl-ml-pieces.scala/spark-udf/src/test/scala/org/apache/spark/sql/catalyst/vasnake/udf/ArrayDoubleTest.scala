/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf


import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql.DataFrame

import com.github.vasnake.spark.test.LocalSpark

class ArrayDoubleTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", "cast(feature_list as array<double>) as feature")
      .orderBy("uid")
      .repartition(4)
  )

  val resultColumnType: String = "ArrayType(DoubleType,true)"

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

  it should "produce null item if all values are invalid, part G" in {
    val input = inputDF.where("part = 'G'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray(7.0, null)")
    minAndCheck(input, "WrappedArray(3.0, null)")
    maxAndCheck(input, "WrappedArray(4.0, null)")
    avgAndCheck(input, "WrappedArray(3.5, null)")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray(7.0, 5.0)")
    minAndCheck(input, "WrappedArray(3.0, 2.0)")
    maxAndCheck(input, "WrappedArray(4.0, 3.0)")
    avgAndCheck(input, "WrappedArray(3.5, 2.5)")
  }

  it should "consider absent values as null, part J" in {
    val input = inputDF.where("part = 'J'")
    show(input, message = "input")
    sumAndCheck(input, "WrappedArray(1.0, 2.0, 3.0)")
    minAndCheck(input, "WrappedArray(1.0, 2.0, 3.0)")
    maxAndCheck(input, "WrappedArray(1.0, 2.0, 3.0)")
    avgAndCheck(input, "WrappedArray(1.0, 2.0, 3.0)")
  }

}
