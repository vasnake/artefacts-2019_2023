/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package com.github.vasnake.spark.udf.`java-api`

import org.scalatest._
import flatspec._
import matchers._

class MapValuesOrderedUDFTest extends AnyFlatSpec with should.Matchers {
  val udf = new MapValuesOrderedUDF()

  it should "produce reference values" in {
    val inputData: Map[String, java.lang.Float] = Map(
      "b" -> null,
      "c" -> Float.NaN,
      "a" -> 3.14f,
      "d" -> Float.NegativeInfinity,
      "e" -> Float.PositiveInfinity
    )

    val keysOrder = Array("a", "b", "c", "d", "e")

    val actual = udf.call(
      kvs = inputData,
      keys = keysOrder
    )

    assert(actual.map(x => s"${x}").toList === List("3.14", "null", "null", "null", "null"))
  }

}
