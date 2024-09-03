/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
import com.github.vasnake.test.{ Conversions => CoreConversions }
import org.scalactic.Equality
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class MultinomialNBTest extends AnyFlatSpec with should.Matchers {
  import MultinomialNBTest._
  import CoreConversions.implicits._
  import CoreConversions.equalityImplicits._

  it should "create new predictor from config" in {
    val config = MultinomialNBConfig(
      featuresLength = 1,
      predictLength = 1,
      classLogPrior = Array(1f),
      featureLogProb = Seq(Array(2d))
    )
    val estimator = MultinomialNB(config)
  }

  it should "fail on empty input" in {
    val config = MultinomialNBConfig(
      featuresLength = 2,
      predictLength = 1,
      classLogPrior = Array(1f),
      featureLogProb = Seq(Array(2d, 3d))
    )
    val estimator = MultinomialNB(config)
    assertThrows[IllegalArgumentException] {
      val output = estimator.predict(Array.empty[Float])
    }
  }

  it should "produce reference predictions" in {
    val config = MultinomialNBConfig(
      featuresLength = 5,
      predictLength = 2,
      classLogPrior = Array(-0.916290731874155, -0.5108256237659905).map(_.toFloat),
      featureLogProb = Seq(
        Array(-3.2188758248682006, -3.2188758248682006, -3.2188758248682006, -0.8209805520698301,
          -0.8209805520698301),
        Array(-1.700409690639827, -1.3109449238781037, -1.3109449238781037, -1.700409690639827,
          -2.3470368555648795)
      ).map(_.map(_.toDouble))
    )
    val estimator = MultinomialNB(config)

    input.indices.foreach { idx =>
      val output = estimator.predict(input(idx))
      assert(output.toSeq.toFloat === expected(idx))
    }
  }

  it should "fail on mismatched features lenght in config" in {
    val config = MultinomialNBConfig(
      featuresLength = 3,
      predictLength = 1,
      classLogPrior = Array(1f),
      featureLogProb = Seq(Array(1d, 2d))
    )
    assertThrows[IllegalArgumentException] {
      val estimator = MultinomialNB(config)
    }
  }

  it should "fail on mismatched predict lenght in config" in {
    val config1 = MultinomialNBConfig(
      featuresLength = 3,
      predictLength = 2,
      classLogPrior = Array(1f),
      featureLogProb = Seq(Array(1d, 2d, 3d), Array(1d, 2d, 3d))
    )
    assertThrows[IllegalArgumentException] {
      val estimator = MultinomialNB(config1)
    }
    val config2 = MultinomialNBConfig(
      featuresLength = 3,
      predictLength = 2,
      classLogPrior = Array(1f, 2f),
      featureLogProb = Seq(Array(1d, 2d, 3d))
    )
    assertThrows[IllegalArgumentException] {
      val estimator = MultinomialNB(config2)
    }
  }
}

object MultinomialNBTest {
  val input: Seq[Array[Float]] = Seq(
    Seq(1, 0, 1, 0, 1),
    Seq(0, 1, 0, 1, 0),
    Seq(0, 0, 1, 1, 0),
    Seq(1, 1, 0, 0, 1),
    Seq(1, 0, 0, 0, 1)
  ).map(_.map(_.toFloat).toArray)

  val expected: Seq[Seq[Float]] = Seq(
    Seq(0.090643, 0.909357),
    Seq(0.192481, 0.807519),
    Seq(0.192481, 0.807519),
    Seq(0.090643, 0.909357),
    Seq(0.401823, 0.598177)
  ).map(_.map(_.toFloat))

  def floatsAreEqual(
    a: Float,
    b: Float
  )(implicit
    tolerance: Float
  ): Boolean =
    (a <= b + tolerance) && (a >= b - tolerance)

  object implicits {
    implicit val tolerance: Float = 0.00001f
    implicit val seqFloatEquals: Equality[Seq[Float]] =
      createSeqFloatsEquality((a, b) => floatsAreEqual(a, b))
  }
}
