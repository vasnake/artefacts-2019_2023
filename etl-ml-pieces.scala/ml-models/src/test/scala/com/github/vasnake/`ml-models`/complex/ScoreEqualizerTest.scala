/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
import com.github.vasnake.test.{ Conversions => CoreConversions }
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class ScoreEqualizerTest extends AnyFlatSpec with should.Matchers {
  import ScoreEqualizerTest._
  import CoreConversions.implicits._

  it should "create new transformer from config" in {
    val config = ScoreEqualizerConfig(
      minval = 0f,
      maxval = 0f,
      noise = 0f,
      eps = 0f,
      coefficients = Seq(Array(1f), Array(2f), Array(3f), Array(4f)).map(_.map(_.toDouble)).toArray,
      intervals = Array(0f, 1f)
    )
    val transformer = ScoreEqualizer(config)
  }

  it should "not fail on empty input" in {
    val config = ScoreEqualizerConfig(
      minval = 0f,
      maxval = 0f,
      noise = 0f,
      eps = 0f,
      coefficients = Seq(Array(1f), Array(2f), Array(3f), Array(4f)).map(_.map(_.toDouble)).toArray,
      intervals = Array(0f, 1f)
    )
    val transformer = ScoreEqualizer(config)

    // assertThrows[IllegalArgumentException] {
    transformer.transform(Array.empty[Float])
    // }
  }

  it should "fail on corrupted config" in {

    assertThrows[IllegalArgumentException] {
      // empty intervals
      val transformer = ScoreEqualizer(
        ScoreEqualizerConfig(
          minval = 0,
          maxval = 0,
          noise = 0,
          eps = 0,
          coefficients = Seq(Array.empty[Double]),
          intervals = Array.empty[Double]
        )
      )
    }

    assertThrows[IllegalArgumentException] {
      // coeffs arrays count != 4
      val transformer = ScoreEqualizer(
        ScoreEqualizerConfig(
          minval = 0f,
          maxval = 0f,
          noise = 0f,
          eps = 0f,
          coefficients = Seq(Array(1f), Array(2f), Array(3f)).map(_.map(_.toDouble)).toArray,
          intervals = Array(0f, 1f)
        )
      )
    }

    assertThrows[IllegalArgumentException] {
      // coeffs arrays not the same length
      val transformer = ScoreEqualizer(
        ScoreEqualizerConfig(
          minval = 0f,
          maxval = 0f,
          noise = 0f,
          eps = 0f,
          coefficients =
            Seq(Array(1f), Array(2f), Array(3f), Array(4f, 5f)).map(_.map(_.toDouble)).toArray,
          intervals = Array(0f, 1f)
        )
      )
    }

    assertThrows[IllegalArgumentException] {
      // intervals count < 2
      val transformer = ScoreEqualizer(
        ScoreEqualizerConfig(
          minval = 0f,
          maxval = 0f,
          noise = 0f,
          eps = 0f,
          coefficients =
            Seq(Array(1f), Array(2f), Array(3f), Array(4f)).map(_.map(_.toDouble)).toArray,
          intervals = Array(0f)
        )
      )
    }

  }

  it should "produce reference transformation" in {
    val input: Array[Float] = Seq(0, 100, 200, 850, 1000, Float.NaN).map(_.toFloat).toArray
    val expected: Seq[Float] = Seq(0, 0, 0.1111, 0.8, 1, Float.NaN).map(_.toFloat)

    val transformer = ScoreEqualizer(
      ScoreEqualizerConfig(
        minval = 100,
        maxval = 900,
        noise = 1e-07,
        eps = 1e-06,
        coefficients = Seq(
          Seq(0.0, 0.0, 0.1111111111111111, 0.2222222222222222, 0.3333333333333333,
            0.4444444444444444, 0.5555555555555556, 0.6666666666666666, 0.7777777777777778,
            0.8888888888888888, 1.0),
          Seq(0.0, 0.0, 0.14814807407404937, 0.25925925925925924, 0.37037037037037035,
            0.48148148148148145, 0.5925925925925927, 0.7037037037037036, 0.8148148888890124,
            0.8888888888897777, 1.0),
          Seq(0.0, 0.07407399999987654, 0.18518518518518517, 0.2962962962962963, 0.4074074074074074,
            0.5185185185185186, 0.6296296296296295, 0.7407408148148396, 0.7777777777804444, 1.0,
            1.0),
          Seq(0.0, 0.1111111111111111, 0.2222222222222222, 0.3333333333333333, 0.4444444444444444,
            0.5555555555555556, 0.6666666666666666, 0.7777777777777778, 0.8888888888888888, 1.0,
            1.0)
        ).map(_.toArray),
        intervals = Array(
          0.0, 1e-06, 0.125001125, 0.25000075, 0.375000375, 0.5, 0.624999625, 0.74999925,
          0.8749988749999998, 0.999999, 0.999999000001, 1.0
        )
      )
    )

    val output: Array[Double] = input.clone()
    transformer.transform(output)

    implicit val seqFloatEquals = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b, 0.01f))
    assert(output.toSeq.toFloat === expected)
  }

  it should "produce reference transformation (2)" in {
    val input: Array[Float] =
      """
        |0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0
        |""".stripMargin.trim.split(',').map(_.toFloat)

    val expected: Seq[Float] =
      """
        |0.0, 0.0, 0.4377145164546739, 0.4377145164546739, 0.0, 0.0, 0.0, 0.0, 0.4377145164546739, 0.4377145164546739, 0.4377145164546739, 0.4377145164546739, 0.0, 0.0, 0.4377145164546739, 0.4377145164546739, 0.0, 0.0, 0.4377145164546739, 0.4377145164546739
        |""".stripMargin.trim.split(',').map(_.toFloat).toSeq

    val transformer = ScoreEqualizer(
      ScoreEqualizerConfig(
        minval = 0,
        maxval = 1,
        // noise =  0.0001,
        noise = 0, // no noise: deterministic behaviour
        eps = 0.001,
        coefficients = """
          |0.0, 0.0,                    0.4377145164546739, 1.0
          |0.0, 0.0,                    0.4377145164551125, 1.0
          |0.0, 1.1213807660226394e-12, 1.0,                1.0
          |0.0, 0.4377145164546739,     1.0,                1.0
          |""".stripMargin.trim.split('\n').toSeq.map(line => line.split(',').map(_.trim.toDouble)),
        intervals = "0.0, 0.001, 0.999, 0.999000000001, 1.0".split(',').map(_.toDouble)
      )
    )

    val output: Array[Double] = input.clone()
    transformer.transform(output)

    implicit val seqFloatEquals = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b, 0.0001f))
    assert(output.toSeq.toFloat === expected)
  }
}

object ScoreEqualizerTest {
  def floatsAreEqual(
    a: Float,
    b: Float,
    tolerance: Float
  ): Boolean =
    (a.isNaN && b.isNaN) || (
      (a <= b + tolerance) && (a >= b - tolerance)
    )
}
