/** Created by vasnake@gmail.com on 2024-08-08
  */
package com.github.vasnake.`ml-core`.models

//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

// testOnly *Binarizer*
class BinarizerTest extends AnyFlatSpec with should.Matchers {
  implicit class SeqOfDouble(ds: Seq[Double]) {
    def toFloat: Seq[Float] = ds.map(_.toFloat)
  }

  it should "create binarizer from config" in {
    val config = BinarizerConfig(threshold = 0.5)
    val transformer = Binarizer(config)
    assert(transformer.config.threshold === 0.5)
  }

  it should "fail on empty input" in {
    val transformer = Binarizer(BinarizerConfig())
    assertThrows[IllegalArgumentException] {
      transformer.transform(Array.empty[Double])
    }
  }

  it should "transform zeros to zeros" in {
    val input = Seq(
      Seq(0, 0, 0),
      Seq(0, 0, 0),
      Seq(0, 0, 0),
    ).map(_.map(_.toDouble).toArray)

    val expected = Seq(0, 0, 0).map(_.toFloat)

    val transformer = Binarizer(BinarizerConfig(threshold = 0.0))

    input.foreach { vec =>
      val output = vec.clone()
      transformer.transform(output)
      assert(output.toSeq.toFloat === expected)
    }
  }

  it should "transform ones to ones" in {
    val input = Seq(
      Seq(1, 1, 1),
      Seq(1, 1, 1),
      Seq(1, 1, 1),
    ).map(_.map(_.toDouble).toArray)

    val expected = Seq(1, 1, 1).map(_.toFloat)

    val transformer = Binarizer(BinarizerConfig(threshold = 0.5))

    input.foreach { vec =>
      val output = vec.clone()
      transformer.transform(output)
      assert(output.toSeq.toFloat === expected)
    }
  }

  it should "transform ones to zeros" in {
    val input = Seq(
      Seq(1, 1, 1),
      Seq(1, 1, 1),
      Seq(1, 1, 1),
    ).map(_.map(_.toDouble).toArray)

    val expected = Seq(0, 0, 0).map(_.toFloat)

    val transformer = Binarizer(BinarizerConfig(threshold = 1))

    input.foreach { vec =>
      val output = vec.clone()
      transformer.transform(output)
      assert(output.toSeq.toFloat === expected)
    }
  }

  it should "transform spectrum" in {
    val input = Seq(
      Seq(0, 0.5, 1),
      Seq(0.1, 0.4, 0.9),
      Seq(0.51, 0.49, 0.99),
    ).map(_.map(_.toDouble).toArray)

    val expected = Seq(
      Seq(0, 0, 1),
      Seq(0, 0, 1),
      Seq(1, 0, 1),
    ).map(_.map(_.toFloat))

    val transformer = Binarizer(BinarizerConfig(threshold = 0.5f))

    input.zipWithIndex.foreach {
      case (vec, idx) =>
        val output = vec.clone()
        transformer.transform(output)
        assert(output.toSeq.toFloat === expected(idx))
    }
  }
}
