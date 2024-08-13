/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package com.github.vasnake.spark.udf.`java-api`

import org.scalatest._
import flatspec._
import matchers._

class MapJoinUDFTest extends AnyFlatSpec with should.Matchers {
  val udf = new MapJoinUDF()

  it should "produce reference values" in {
    val inpData: Map[String, java.lang.Float] = Map(
      "a" -> 3.14f,
      "b" -> null,
      "c" -> Float.NaN,
      "d" -> Float.NegativeInfinity,
      "e" -> Float.PositiveInfinity
    )

    val actual = udf.call(
      feature = inpData,
      itemsSep = ",",
      kvSep = ":"
    )

    assert(actual.split(",").sorted.mkString(",") === "a:3.14,b:null,c:null,d:null,e:null")
  }

}
