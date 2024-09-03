/** Created by vasnake@gmail.com on 2024-08-08
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.test.{ Conversions => CoreConversions }
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

// testOnly *Imputer*
class ImputerTest extends AnyFlatSpec with should.Matchers {
  import CoreConversions.implicits._
  import Conversions.implicits._

  it should "create imputer from array of float" in {
    val input = Seq(-1, 0, 1, 2, 3).map(_.toDouble)
    val iv_list = input.toArray
    val imputer = Imputer(iv_list)
    assert(imputer.imputedValues.toSeq.toFloat === input.toFloat)
  }

  it should "transform input numbers to the same numbers" in {
    val iv_list = Seq(-1, 0, 1, 2, 3).map(_.toDouble).toArray
    val imputer = Imputer(iv_list)
    val input = Seq(0, 1, 2, 3, 4).map(_.toDouble)

    val output = input.toArray.clone()
    imputer.transform(output)
    assert(output.toSeq.toFloat === input.toFloat)

    val output2 = Array(1, 2, 3, 4, 5).map(_.toDouble)
    Imputer(iv_list).transform(output2)
    assert(output2.toSeq.toFloat === Seq(1, 2, 3, 4, 5).map(_.toFloat))
  }

  it should "transform zeros to zeros" in {
    val iv_list = Seq(-1, 0, 1, 2, 3).map(_.toDouble)
    val imputer = Imputer(iv_list.toArray)
    val input = Seq(0, 0, 0, 0, 0).map(_.toDouble)

    val output = input.toArray.clone()
    imputer.transform(output)
    assert(output.toSeq.toFloat === input.toFloat)
  }

  it should "transform nan to imputed values" in {
    val iv_list = Seq(-1, 0, 1, 2, 3).map(_.toDouble)
    val imputer = Imputer(iv_list.toArray)
    val input = (1 to 5).map(_ => Double.NaN)

    val output = input.toArray.clone()
    imputer.transform(output)
    assert(output.toSeq.toFloat === iv_list.toFloat)
  }

  it should "replace nan and leave numbers" in {
    val iv_list = Seq(10, 11, 12, 13, 14).map(_.toDouble)
    val imputer = Imputer(iv_list.toArray)
    val input = Seq(0, Double.NaN, 2, Double.NaN, 4).map(_.toDouble)

    val output = imputer._transform(input.toArray)
    assert(output.toSeq.toFloat === Seq(0, 11, 2, 13, 4).map(_.toFloat))
  }

  it should "fail on mismatched array shape" in {
    val iv_list = Seq(10, 11, 12).map(_.toDouble)
    val input = Seq(0, Double.NaN, 2, Double.NaN, 4).map(_.toDouble)
    assert(iv_list.length != input.length)

    val imputer = Imputer(iv_list.toArray)
    assertThrows[IllegalArgumentException] {
      imputer._transform(input.toArray)
    }
  }
}
