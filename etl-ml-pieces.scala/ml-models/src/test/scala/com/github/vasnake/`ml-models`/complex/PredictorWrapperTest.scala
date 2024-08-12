/**
 * Created by vasnake@gmail.com on 2024-08-12
 */
package com.github.vasnake.`ml-models`.complex

import org.scalatest._
import flatspec._
import matchers._
import org.scalactic.Equality

import com.github.vasnake.test.{Conversions => CoreConversions}
import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
import com.github.vasnake.`ml-core`.models._
import com.github.vasnake.`ml-core`.models.interface.Estimator
import com.github.vasnake.common.file.FileToolbox

class PredictorWrapperTest extends AnyFlatSpec with should.Matchers {

  import CoreConversions.implicits._
  import PredictorWrapperTest._

  it should "fail on empty input" in {
    val config = PredictorWrapperConfig(
      minFeaturesPerSample = 1,
      maxFeaturesPerSample = 3,
      predictLength = 2
    )
    val predictor = DummyPredictor(predictLength = 2)
    val wrapper = PredictorWrapper(predictor, config)

    assertThrows[IllegalArgumentException] {
      wrapper.predict(Array.empty[Float])
    }
  }

  it should "produce nan values according to min-max parameters" in {
    val expected: Seq[Seq[Float]] = Seq(
      Seq(nan, nan),
      Seq(nan, nan)
    ) ++ Seq(
      Seq(0.5, 0.5),
      Seq(0.5, 0.5),
      Seq(0.5, 0.5)
    ).map(_.map(_.toFloat))

    val predictor = DummyPredictor(predictLength = 2)
    val config = PredictorWrapperConfig(
      minFeaturesPerSample = 1,
      maxFeaturesPerSample = 3,
      predictLength = 2)

    val wrapper = PredictorWrapper(predictor, config)

    implicit val eq = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b)(0.000001f))

    input.indices.foreach(idx => {
      val output: Array[Double] = wrapper.predict(input(idx))
      assert(output.toSeq.toFloat === expected(idx))
    })
  }

  it should "produce reference predictions using SGDClassifier" in {
    val expected: Seq[Seq[Float]] = Seq(
      Seq(nan, nan, nan),
      Seq(nan, nan, nan)
    ) ++ Seq(
      Seq(0.332482, 0.334245, 0.333273),
      Seq(0.331098, 0.333233, 0.335669),
      Seq(0.324599, 0.334442, 0.340959)
    ).map(_.map(_.toFloat))

    val predictor = {
      val config = PMMLEstimatorConfig(
        featuresLength = 4,
        predictLength = 3,
        fileName = FileToolbox.getResourcePath(this, "/sgd_classifier.pmml")
      )
      SGDClassifier(config)
    }

    val wrapper = {
      val config = PredictorWrapperConfig(
        minFeaturesPerSample = 1,
        maxFeaturesPerSample = 3,
        predictLength = 3
      )
      predictor.init()
      PredictorWrapper(predictor, config)
    }

    implicit val seqFloatEquals = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b)(0.0001f))

    input.indices.foreach(idx => {
      val output: Array[Double] = wrapper.predict(input(idx))
      assert(output.toSeq.toFloat === expected(idx))
    })
  }

}

object PredictorWrapperTest {

  val input: Seq[Array[Float]] = Seq(
    Seq(0, 0, 0, 0),  // features-per-sample < min
    Seq(2, 3, 4, 5),  // features-per-sample > max
    Seq(6, 0, 0, 0),  // features-per-sample between min and max
    Seq(7, 0, 8, 0),
    Seq(0, 9, 8, 7)
  ).map(_.map(_.toFloat).toArray)

  val nan: Float = Float.NaN

  def floatsAreEqual(a: Float, b: Float)(implicit tolerance: Float): Boolean = {
    (a.isNaN && b.isNaN) || (a <= b + tolerance) && (a >= b - tolerance)
  }

  object implicits {
    implicit val tolerance: Float = 0.0001f
    implicit val seqFloatEquals: Equality[Seq[Float]] = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b))
  }

}

case class DummyPredictor(predictLength: Int) extends Estimator {
  override def predict(features: Array[Double]): Array[Double] = {
    (0 until predictLength).map(_ => 0.5).toArray
  }
}
