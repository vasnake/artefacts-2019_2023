/**
 * Created by vasnake@gmail.com on 2024-08-08
 */
package com.github.vasnake.`ml-core`.models

import org.scalatest._
import flatspec._
import matchers._

// testOnly *Tfidf*
class GroupedFeaturesTfidfTest extends AnyFlatSpec  with should.Matchers {

  import GroupedFeaturesTfidfTransformerTest._
  import Conversions.implicits._
  import Conversions.equalityImplicits._

  it should "create transformer from parameters" in {
    val config = GroupedFeaturesTfidfTransformerConfig(
      groups = Seq(Seq(0, 2), Seq(1, 3)).map(_.toArray).toArray,
      idf_diags = Seq(Seq(1.0, 1.51082), Seq(1.22314, 1.0)).map(_.toArray).toArray,
      n_features = 4,
      transformer_params = defaultTransformerParams
    )
    val transformer = GroupedFeaturesTfidfTransformer(config)
    assert(transformer.config.n_features === 4)
  }

  it should "transform no groups" in {
    // no transformation
    val config = GroupedFeaturesTfidfTransformerConfig(
      groups = Array.empty,
      idf_diags = Array.empty,
      n_features = 3,
      transformer_params = defaultTransformerParams
    )
    val input = defaultInput.map(_.toDouble.toArray)
    val transformer = GroupedFeaturesTfidfTransformer(config)
    val expected = input.map(_.toSeq.toFloat)

    input.zipWithIndex.foreach { case (vec, idx) => {
      val output = transformer._transform(vec).toSeq
      output should contain theSameElementsAs expected(idx)
      assert(output === expected(idx))

      val inp = vec.clone()
      transformer.transform(inp)
      assert(inp.toSeq.toFloat === expected(idx))
    }}
  }

  it should "transform all features in one group" in {
    val config = GroupedFeaturesTfidfTransformerConfig(
      groups = Seq(Seq(0, 1, 2)).map(_.toArray).toArray,
      idf_diags = Seq(Seq(1f, 1f, 1f).toDouble).map(_.toArray).toArray,
      n_features = 3,
      transformer_params = defaultTransformerParams
    )
    val input = defaultInput.map(_.toDouble.toArray)
    val expected = input.map(vec => vec.map(x => x / 10f).toSeq.toFloat)
    val transformer = GroupedFeaturesTfidfTransformer(config)

    input.zipWithIndex.foreach { case (vec, idx) => {
      val output = transformer._transform(vec).toSeq
      output.toFloat should contain theSameElementsAs expected(idx)
      assert(output.toFloat === expected(idx))

      val inp = vec.clone()
      transformer.transform(inp)
      assert(inp.toSeq.toFloat === expected(idx))
    }}
  }

  it should "transform one group with two features" in {
    val config = GroupedFeaturesTfidfTransformerConfig(
      groups = Seq(Seq(0, 2)).map(_.toArray).toArray,
      idf_diags = Seq(Seq(1f, 1f).toDouble).map(_.toArray).toArray,
      n_features = 3,
      transformer_params = defaultTransformerParams
    )
    val input = defaultInput.map(_.toDouble.toArray)
    val expected = defaultExpected
    val transformer = GroupedFeaturesTfidfTransformer(config)

    input.zipWithIndex.foreach { case (vec, idx) => {
      val output = transformer._transform(vec).toSeq
      output.toFloat should contain theSameElementsAs expected(idx)
      assert(output.toFloat === expected(idx))

      val inp = vec.clone()
      transformer.transform(inp)
      assert(inp.toSeq.toFloat === expected(idx))
    }}
  }

  it should "transform two groups with two features each" in {
    val config = GroupedFeaturesTfidfTransformerConfig(
      groups = Seq(Seq(0, 2), Seq(1, 3)).map(_.toArray).toArray,
      idf_diags = Seq(Seq(1f, 1.510826f).toDouble, Seq(1f, 1f).toDouble).map(_.toArray).toArray,
      n_features = 4,
      transformer_params = defaultTransformerParams
    )
    val input = Seq(Array(2f, 6f, 14f, 10f).map(_.toDouble))
    val expected = Seq(Seq(0.086387f, 0.375f, 0.913613f, 0.625f))
    val transformer = GroupedFeaturesTfidfTransformer(config)

    input.zipWithIndex.foreach { case (vec, idx) => {
      val output = transformer._transform(vec).toSeq
      output.toFloat should contain theSameElementsAs expected(idx)
      assert(output.toFloat === expected(idx))

      val inp = vec.clone()
      transformer.transform(inp)
      assert(inp.toSeq.toFloat === expected(idx))
    }}
  }

  it should "fail on mismatched input size" in {
    val config = GroupedFeaturesTfidfTransformerConfig(
      groups = Seq(Seq(0, 2), Seq(1, 3)).map(_.toArray).toArray,
      idf_diags = Seq(Seq(1f, 1.510826f).toDouble, Seq(1f, 1f).toDouble).map(_.toArray).toArray,
      n_features = 4,
      transformer_params = defaultTransformerParams
    )
    val input = Seq(Array(1f, 2f, 3f).map(_.toDouble))
    val transformer = GroupedFeaturesTfidfTransformer(config)

    input.zipWithIndex.foreach { case (vec, _) => {
      assertThrows[IllegalArgumentException] {
        transformer._transform(vec)
      }
      assertThrows[IllegalArgumentException] {
        transformer.transform(vec)
      }
    }}
  }

}

object GroupedFeaturesTfidfTransformerTest {

  val defaultTransformerParams = Map(
    "norm"          -> "l1",
    "smooth_idf"    -> "true",
    "sublinear_tf"  -> "false",
    "use_idf"       -> "true"
  )

  val defaultInput = Seq(
    Seq(1f, 8f, 1f),
    Seq(6f, 2f, 2f),
    Seq(1f, 6f, 3f),
    Seq(7f, 2f, 1f),
    Seq(2f, 5f, 3f)
  )

  val defaultExpected = Seq(
    Seq(0.5f,   8f, 0.5f),
    Seq(0.75f,  2f, 0.25f),
    Seq(0.25f,  6f, 0.75f),
    Seq(0.875f, 2f, 0.125f),
    Seq(0.4f,   5f, 0.6f)
  )

}
