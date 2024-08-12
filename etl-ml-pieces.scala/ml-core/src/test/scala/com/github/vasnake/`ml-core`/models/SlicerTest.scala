/**
 * Created by vasnake@gmail.com on 2024-08-12
 */
package com.github.vasnake.`ml-core`.models

import org.scalatest._
import flatspec._
import matchers._
import org.scalactic.Equality

import com.github.vasnake.test.{Conversions => CoreConversions}
import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality

class SlicerTest extends AnyFlatSpec with should.Matchers {

  import CoreConversions.implicits._
  import SlicerTest._
  import SlicerTest.implicits._

  it should "fail on none columns" in {
    assertThrows[IllegalArgumentException] {
      val transformer = Slicer(columns=Array.empty[Int])
    }
  }

  it should "fail on empty input" in {
    val input = Array.empty[Float]
    val transformer = Slicer(columns=Array(1))

    assertThrows[IllegalArgumentException] {
      val output: Array[Float] = transformer.transform(input)
    }
  }

  it should "slice one column" in {
    val expected: Seq[Seq[Float]] = Seq(
      Seq(2),
      Seq(12),
      Seq(22)
    ).map(_.map(_.toFloat))

    val transformer = Slicer(columns=Array(1))

    input.indices.foreach(idx => {
      val output: Array[Float] = transformer.transform(input(idx))
      assert(output.toSeq === expected(idx))
    })
  }

  it should "slice a set of different columns" in {
    val expected: Seq[Seq[Float]] = Seq(
      Seq(4,  2),
      Seq(14, 12),
      Seq(24, 22)
    ).map(_.map(_.toFloat))

    val transformer = Slicer(columns=Array(3, 1))

    input.indices.foreach(idx => {
      val output: Array[Float] = transformer.transform(input(idx))
      assert(output.toSeq === expected(idx))
    })
  }

  it should "slice repeated columns" in {
    val expected: Seq[Seq[Float]] = Seq(
      Seq(3,  3),
      Seq(13, 13),
      Seq(23, 23)
    ).map(_.map(_.toFloat))

    val transformer = Slicer(columns=Array(2, 2))

    input.indices.foreach(idx => {
      val output: Array[Float] = transformer.transform(input(idx))
      assert(output.toSeq === expected(idx))
    })
  }

}

object SlicerTest {

  val input: Seq[Array[Float]] = Seq(
    Seq(1, 2, 3, 4, 5),
    Seq(11, 12, 13, 14, 15),
    Seq(21, 22, 23, 24, 25)
  ).map(_.map(_.toFloat).toArray)

  def floatsAreEqual(a: Float, b: Float)(implicit tolerance: Float): Boolean = {
    (a <= b + tolerance) && (a >= b - tolerance)
  }

  object implicits {
    implicit val tolerance: Float = 0.0000001f
    implicit val seqFloatEquals: Equality[Seq[Float]] = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b))
  }

}
