/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`ml-core`.models.interface._
import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
import com.github.vasnake.test.{ Conversions => CoreConversions }
import org.scalactic.Equality
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class GroupedTransformerTest extends AnyFlatSpec with should.Matchers {
  import GroupedTransformerTest._
  import CoreConversions.implicits._

  it should "create new transformer from config" in {
    val config = SBGroupedTransformerConfig(
      groups = Map("" -> DummyTransformerConfig(0f))
    )

    val transformer = SBGroupedTransformer(
      config,
      group = "",
      transformerFactory = { _ => DummyTransformer(0f) }
    )
  }

  it should "fail on mismatched groups" in {
    val config = SBGroupedTransformerConfig(
      groups = Map("group2" -> DummyTransformerConfig(outputValue = 2f))
    )

    val ex = intercept[IllegalArgumentException] {
      val transformer = SBGroupedTransformer(
        config,
        group = "group1",
        transformerFactory = transformersFactory
      )
    }
    assert(ex.getMessage.contains("can't find transformer 'group1'"))
  }

  it should "fail on empty input" in {
    val transformer = SBGroupedTransformer(
      SBGroupedTransformerConfig(groups = Map.empty),
      group = "",
      transformersFactory
    )

    val ex = intercept[IllegalArgumentException] {
      transformer.transform(Array.empty[Float])
    }
    assert(ex.getMessage.contains("input must not be empty"))
  }

  it should "produce simple transformation" in {
    val input: Array[Float] = Array(42f)
    val transformerGroups = Map(
      "group1" -> DummyTransformerConfig(outputValue = 1f),
      "group2" -> DummyTransformerConfig(outputValue = 2f)
    )

    Seq(
      // apply each transformer: transformer1, transformer2, no-transformer
      ("group1", 1f),
      ("group2", 2f),
      ("", 42f)
    ).foreach {
      case (group, expected) =>
        val transformer = SBGroupedTransformer(
          SBGroupedTransformerConfig(groups = transformerGroups),
          group,
          transformersFactory
        )

        assert(output(transformer, input) === Seq(expected))
    }
  }

  it should "produce reference transformation" in {
    val config = SBGroupedTransformerConfig(
      Map(
        "group1" -> DummyTransformerConfig(outputValue = 1f),
        "group2" -> DummyTransformerConfig(outputValue = 2f)
      )
    )

    val transformer = SBGroupedTransformer(
      config,
      "group1",
      transformersFactory
    )

    val input = Array(0, 1, 2) map tofloat
    val expected = Seq(1, 1, 1) map tofloat

    assert(output(transformer, input) === expected)
  }
}

object GroupedTransformerTest {
  import CoreConversions.implicits._

  case class DummyTransformerConfig(outputValue: Float) extends PostprocessorConfig

  case class DummyTransformer(output_value: Float) extends InplaceTransformer {
    override def transform(vec: Array[Double]): Unit =
      vec.indices.foreach(i => vec(i) = output_value)
  }

  val transformersFactory: PostprocessorConfig => InplaceTransformer =
    conf =>
      DummyTransformer(
        output_value = conf.asInstanceOf[DummyTransformerConfig].outputValue
      )

  val tofloat: Int => Float = x => x.toFloat

  def output(tr: InplaceTransformer, input: Array[Float]): Seq[Float] = {
    val o = input.map(_.toDouble)
    tr.transform(o)
    o.toSeq.toFloat
  }

  implicit val seqFloatEquals: Equality[Seq[Float]] =
    createSeqFloatsEquality((a, b) => floatsAreEqual(a, b, 0.00001f))

  def floatsAreEqual(
    a: Float,
    b: Float,
    tolerance: Float
  ): Boolean =
    (a.isNaN && b.isNaN) || (
      (a <= b + tolerance) && (a >= b - tolerance)
    )
}
