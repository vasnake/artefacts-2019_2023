/**
 * Created by vasnake@gmail.com on 2024-08-08
 */
package com.github.vasnake.`ml-core`.models

import org.scalatest._
import flatspec._
import matchers._

// testOnly *Scaler*
class ScalerTest extends AnyFlatSpec with should.Matchers {

  import ScalerTest._
  import Conversions.implicits._
  import Conversions.equalityImplicits._

  it should "create new transformer from config" in {
    val config = ScalerConfig(
      withMean = true, withStd = true,
      means = Array(1f, 2f, 3f),
      scales = Array(4f, 5f, 6f)
    )
    val transformer = Scaler(config)
    assert(transformer.config.withMean === true)
  }

  it should "fail on empty input" in {
    val transformer = Scaler(ScalerConfig())
    assertThrows[IllegalArgumentException] {
      transformer.transform(Array.empty[Double])
    }
  }

  it should "transform all zeros" in {
    val input = Seq(0, 0, 0, 0).map(_.toDouble).toArray
    val expected = Seq(0.0, -0.827759, -2.44949, -0.710398).map(_.toFloat)
    val config = defaultConfig

    val transformer = Scaler(config)
    val output = transformer._transform(input)
    assert(output.toSeq.toFloat === expected)
  }

  it should "transform all ones" in {
    val input = Seq(1, 1, 1, 1).map(_.toDouble).toArray
    val expected = Seq(1.0, 1.409428, -2.437242, -0.708273).map(_.toFloat)
    val config = defaultConfig

    val transformer = Scaler(config)
    val output = transformer._transform(input)
    assert(output.toSeq.toFloat === expected)
  }

  it should "fail on mismatched shapes" in {
    val input = Seq(1, 1, 1).map(_.toDouble).toArray
    val config = defaultConfig
    val transformer = Scaler(config)

    assertThrows[IllegalArgumentException] {
      transformer.transform(input)
    }
  }

  it should "transform spectrum with mean+std" in {
    val input = defaultInput
    val expected = Seq(7.0, 10.358175, -2.448265, -0.687027).map(_.toFloat)
    val config = defaultConfig.copy(withMean = true, withStd = true)

    val transformer = Scaler(config)
    val output: Array[Double] = transformer._transform(input)
    assert(output.toSeq.toFloat === expected)
  }

  it should "transform spectrum with nomean+nostd" in {
    val input = defaultInput
    val expected = defaultInput.toSeq
    val config = defaultConfig.copy(withMean = false, withStd = false)

    val transformer = Scaler(config)
    val output = transformer._transform(input)
    assert(output.toSeq === expected)
  }

  it should "transform spectrum with mean+nostd" in {
    val input = defaultInput
    val expected = Seq(7.0, 4.63, -199.9, -323.366667).map(_.toFloat)
    val config = defaultConfig.copy(withMean = true, withStd = false)

    val transformer = Scaler(config)
    val output: Array[Double] = transformer._transform(input)
    assert(output.toSeq.toFloat === expected)
  }

  it should "transform spectrum with nomean+std" in {
    val input = defaultInput
    val expected = Seq(7.0, 11.18593, 1.2247e-03, 2.337e-02).map(_.toFloat)
    val config = defaultConfig.copy(withMean = false, withStd = true)

    val transformer = Scaler(config)
    val output: Array[Double] = transformer._transform(input)
    assert(output.toSeq.toFloat === expected)
  }

}

object ScalerTest {

  def defaultConfig: ScalerConfig = ScalerConfig(
    withMean = true, withStd = true,
    means = Array(0.0, 0.370, 200.0, 334.366666).map(_.toDouble),
    scales = Array(1.0, 0.4469899327, 81.64965809, 470.6753327).map(_.toDouble)
  )

  def defaultInput: Array[Float] = Seq(7, 5, 0.1, 11).map(_.toFloat).toArray

}
