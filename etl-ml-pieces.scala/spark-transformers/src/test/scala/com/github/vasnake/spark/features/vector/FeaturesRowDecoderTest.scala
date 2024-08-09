/**
 * Created by vasnake@gmail.com on 2024-08-08
 */
package com.github.vasnake.spark.features.vector

import com.github.vasnake.spark.test.SimpleLocalSpark
import com.github.vasnake.`ml-core`.models.Conversions
import com.github.vasnake.`etl-core`.{FeaturesGroup, GroupedFeatures}
import com.github.vasnake.`ml-core`.models.EqualityCheck.createSeqFloatsEquality
//import com.github.vasnake.spark.features.vector.FeaturesRowDecoder
//import com.github.vasnake.core.text.StringToolbox
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalactic.Equality

import scala.collection.mutable
import org.scalatest._
import flatspec._
import matchers._

// testOnly *RowDecoder*
class FeaturesRowDecoderTest extends AnyFlatSpec  with should.Matchers with SimpleLocalSpark {

//  import VectorizationTest._
  import FeaturesRowDecoderTest._
  import Conversions.implicits._

  it should "throw exception on empty DataFrame" in {
    import spark.implicits._
    val df = Seq("").toDF
    val gfs = GroupedFeatures(Seq(FeaturesGroup("column", Array("idx1", "idx2", "idx3"))))

    assertThrows[IllegalArgumentException] {
      FeaturesRowDecoder(df.schema, gfs).decode(df.head)
    }
  }

  it should "extract vector of nan's from null column" in {
    // all features is nan from null column only for Array; for Map null column produces zeros
    import spark.implicits._

    val df = Seq(GrinderDatasetRow(
      uid = "a",
      topics_motor200 = None,
      vk_groups_all = None,
      all_profs = None
    )).toDF

    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_1", "topic_2", "topic_3")),
      FeaturesGroup("all_profs", Array("0", "1", "2"))
    ))

    val result = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(result.length === 6)
    assert(result.take(3).forall(_ == 0))
    assert(result.drop(3).forall(_.isNaN))
  }

  it should "extract vector of zero's for non-existent features" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_11", "topic_12", "topic_13")),
      FeaturesGroup("all_profs", Array("10", "11", "12", "13", "14"))
    ))

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 8)
    assert(res.take(3).forall(_ == 0.0f))  // map collection
    assert(res.drop(3).forall(_.isNaN))  // array collection
  }

  it should "extract empty vector for empty GroupedFeatures" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq.empty)

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 0)
  }

  it should "extract empty vector for empty features list" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(FeaturesGroup("all_profs", Array.empty)))

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 0)
  }

  it should "extract ordered list of features" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_1", "topic_2", "topic_3")),
      FeaturesGroup("all_profs", Array("0", "1", "2", "3", "4"))
    ))

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 8)
    assert(res.toSeq === Seq(0.1, 0.2, 0.3, 0.99, 0.98, 0.97, 0.96, 0.95).map(_.toFloat))
  }

  it should "extract unordered list of features" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_3", "topic_1", "topic_2")),
      FeaturesGroup("all_profs", Array("3", "1", "2", "0", "4"))
    ))

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 8)
    assert(res.toSeq === Seq(0.3, 0.1, 0.2, 0.96, 0.98, 0.97, 0.99, 0.95).map(_.toFloat))
  }

  it should "extract features subset" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_3", "topic_1")),
      FeaturesGroup("all_profs", Array("3", "1", "1"))
    ))

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 5)
    assert(res.toSeq === Seq(0.3, 0.1, 0.96, 0.98, 0.98).map(_.toFloat))
  }

  it should "extract and slice merged grouped features" in {
    import spark.implicits._
    val df = input.toDF
    val gfs1 = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_3", "topic_1")),
      FeaturesGroup("all_profs", Array("3", "1", "1"))
    ))
    val gfs2 = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_3", "topic_2")),
      FeaturesGroup("all_profs", Array("2", "3"))
    ))
    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs1, gfs2))

    val allFeatures = FeaturesRowDecoder(df.schema, mergedGfs).decode(df.head)
    assert(allFeatures.length === 6)

    val ids1 = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs1)
    val fs1 = GroupedFeatures.sliceFeaturesVector(allFeatures, ids1)
    assert(fs1.toSeq === Seq(0.3, 0.1, 0.96, 0.98, 0.98).map(_.toFloat))

    val ids2 = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs2)
    val fs2 = GroupedFeatures.sliceFeaturesVector(allFeatures, ids2)
    assert(fs2.toSeq === Seq(0.3, 0.2, 0.97, 0.96).map(_.toFloat))
  }

  it should "extract features for multiple models" in {
    import spark.implicits._
    val df = input.toDF
    val gfs1 = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_1", "topic_2")),
      FeaturesGroup("all_profs", Array(0, 2).map(_.toString))
    ))
    val gfs2 = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_3", "topic_1")),
      FeaturesGroup("all_profs", Array(2, 1).map(_.toString))
    ))
    val expected1 = Seq(0.1, 0.2, 0.99, 0.97).map(_.toFloat)
    val expected2 = Seq(0.3, 0.1, 0.97, 0.98).map(_.toFloat)

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs1, gfs2))
    val allFeatures = FeaturesRowDecoder(df.schema, mergedGfs).decode(df.head)
    assert(allFeatures.length === 6)

    val ids1 = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs1)
    val fs1 = GroupedFeatures.sliceFeaturesVector(allFeatures, ids1)
    assert(fs1.toSeq === expected1)

    val ids2 = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs2)
    val fs2 = GroupedFeatures.sliceFeaturesVector(allFeatures, ids2)
    assert(fs2.toSeq === expected2)
  }

  it should "extract double features" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("vk_groups_all", Array("1", "3"))
    ))

    val res = FeaturesRowDecoder(df.schema, gfs).decode(df.head)

    assert(res.length === 2)
    assert(res.toSeq === Seq(1, 3).map(_.toFloat))
  }

  it should "extract nan for null topics" in {
    // nan will be extracted from map only if feature value is null, 0 otherwise

    val row: Row = {
      import org.apache.spark.sql.types._
      new GenericRowWithSchema(Seq(
        Map("topic_1" -> null),
        mutable.WrappedArray.make(Array(0.1, null))
      ), StructType(Seq(
        StructField("topics_motor200", MapType(DataTypes.StringType, DataTypes.DoubleType)),
        StructField("all_profs", ArrayType(DataTypes.DoubleType))
      )))
    }

    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_1", "no-topic")),
      FeaturesGroup("all_profs", Array(0, 1, 2).map(_.toString))
    ))
    val expected = Seq(nan, 0.0, 0.1, nan, nan).map(_.toFloat)

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))
    val features = FeaturesRowDecoder(row.schema, mergedGfs).decode(row)
    val indices = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance

    assert(res.toSeq.toFloat === expected)
  }

  it should "extract nan for null all_profs" in {
    import spark.implicits._
    val df = input.map(r => r.copy(all_profs = None)).toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_1", "topic_200", "topic_3")),
      FeaturesGroup("all_profs", Array(0, 1, 2).map(_.toString))
    ))
    val expected = Seq(0.1, 0.0, 0.3, nan, nan, nan).map(_.toFloat)

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))
    val features = FeaturesRowDecoder(df.schema, mergedGfs).decode(df.head)
    val indices = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)

    assert(res.length === 6)
    assert(res.drop(3).forall(_.isNaN))
    assert(res.take(3).toSeq === expected.take(3))
  }

  it should "not extract duplicated features" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_1", "topic_3", "topic_1")),
      FeaturesGroup("all_profs", Array(0, 2, 2).map(_.toString))
    ))
    val expected = Seq(0.1, 0.3, 0.1, 0.99, 0.97, 0.97).map(_.toFloat)

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))
    val features = FeaturesRowDecoder(df.schema, mergedGfs).decode(df.head)

    assert(features.length === 4)

    val indices = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)

    assert(res.length === 6)
    assert(res.toSeq === expected)
  }

  it should "extract features in order" in {
    import spark.implicits._
    val df = input.toDF
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("topics_motor200", Array("topic_2", "topic_1", "topic_3")),
      FeaturesGroup("all_profs", Array(3, 0, 2).map(_.toString))
    ))
    val expected = Seq(0.2, 0.1, 0.3, 0.96, 0.99, 0.97).map(_.toFloat)

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))
    val features = FeaturesRowDecoder(df.schema, mergedGfs).decode(df.head)

    assert(features.length === 6)

    val indices = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === expected)
  }

  it should "use custom default values" in {
    // cases: no field, field is null, no feature key, null under key.
    // no field case: throw an exception, no need for default value;

    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("f_map_nullfield", Array("idx")),
      FeaturesGroup("f_array_nullfield", Array("0")),
      FeaturesGroup("f_map_values", Array("no-key", "fnull")),
      FeaturesGroup("f_array_values", Array("1", "3"))  // null, no-key
    ))

    val defaults = Map(
      "f_map_nullfield"   -> FieldDefaults(nullField = 1f, noFeatureKey = 11f, nullFeature = 111f),
      "f_array_nullfield" -> FieldDefaults(nullField = 2f, noFeatureKey = 22f, nullFeature = 222f),
      "f_map_values"      -> FieldDefaults(nullField = 33f, noFeatureKey = 3f, nullFeature = 4f),
      "f_array_values"    -> FieldDefaults(nullField = 44f, noFeatureKey = 6f, nullFeature = 5f)
    )

    val expected = Seq(1, 2, 3, 4, 5, 6).map(_.toFloat)

    // TODO: implement custom defaults in decoder
    val features = FeaturesRowDecoder(dfRowWithAllVariations.schema, gfs).decode(dfRowWithAllVariations)
    //assert(features.toSeq.toFloat === expected)
  }

  it should "extract null Double values as NaN for Array columns" in {

    val dataRow: Row = {
      import org.apache.spark.sql.types._
      new GenericRowWithSchema(Seq(
        mutable.WrappedArray.make(Array(0.0, null, 42.0))
      ), StructType(Seq(
        StructField("d_array", ArrayType(DataTypes.DoubleType))
      )))
    }

    val gfs = GroupedFeatures(Seq(FeaturesGroup("d_array", Array("0", "1", "2"), isArray = true)))
    val expected: Seq[Float] = Seq(0f, Float.NaN, 42f)

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))
    val allFeatures = FeaturesRowDecoder(dataRow.schema, mergedGfs).decode(dataRow)
    val indices = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    val modelFeatures = GroupedFeatures.sliceFeaturesVector(allFeatures, indices)

    implicit val eq: Equality[Seq[Float]] = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(modelFeatures.toSeq.toFloat === expected)
  }

  it should "abide by the rules of vectorization: fail if can't find field in dataframe" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("no_field", Array("0"))
    ))

    val err = intercept[IllegalArgumentException] {
      val vector = extractVector(gfs, dfRowWithAllVariations)
    }
    assert(err.getMessage.take(31) === """Field "no_field" does not exist""")
  }

  it should "abide by the rules of vectorization: fail if field in dataframe have unknown type" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("s_map_field", Array("0"))
    ))

    val err = intercept[IllegalArgumentException] {
      val vector = extractVector(gfs, dfRowWithAllVariations)
    }
    // Unknown type 'MapType(StringType,StringType,true)' of collection column with name 's_map_field'
    assert(err.getMessage.take(12) === """Unknown type""")
  }

  it should "abide by the rules of vectorization: MapType, field is null => all features = 0" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("d_map_nullfield", Array("0", "1", "2"), isArray = false),
      FeaturesGroup("f_map_nullfield", Array("0", "1", "2"), isArray = false)
    ))
    val expected: Seq[Float] = for (_ <- 0 to 5) yield 0f

    val vector = extractVector(gfs, dfRowWithAllVariations)

    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(vector.toSeq.toFloat === expected)
  }

  it should "abide by the rules of vectorization: MapType, missing key => feature = 0" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("d_map_values", Array("no-key-0", "no-key-1", "no-key-2"), isArray = false),
      FeaturesGroup("f_map_values", Array("no-key-0", "no-key-1", "no-key-2"), isArray = false)
    ))
    val expected: Seq[Float] = for (_ <- 0 to 5) yield 0f

    val vector = extractVector(gfs, dfRowWithAllVariations)

    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(vector.toSeq.toFloat === expected)
  }

  it should "abide by the rules of vectorization: MapType, null under key => feature is NaN" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("d_map_values", Array("fnull", "f42"), isArray = false),
      FeaturesGroup("f_map_values", Array("fnull", "f42"), isArray = false)
    ))
    val expected: Seq[Float] = Seq(Float.NaN, 42f, Float.NaN, 42f)

    val vector = extractVector(gfs, dfRowWithAllVariations)

    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(vector.toSeq.toFloat === expected)
  }

  it should "abide by the rules of vectorization: ArrayType, field is null => all features is NaN" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("d_array_nullfield", Array("0", "1", "2"), isArray = true),
      FeaturesGroup("f_array_nullfield", Array("0", "1", "2"), isArray = true)
    ))
    val expected: Seq[Float] = for (_ <- 0 to 5) yield Float.NaN

    val vector = extractVector(gfs, dfRowWithAllVariations)

    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(vector.toSeq.toFloat === expected)
  }

  it should "abide by the rules of vectorization: ArrayType, missing key => feature is NaN" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("d_array_values", Array("2", "3", "4"), isArray = true),
      FeaturesGroup("f_array_values", Array("2", "3", "4"), isArray = true)
    ))
    val expected: Seq[Float] = for (_ <- 0 to 5) yield Float.NaN

    val vector = extractVector(gfs, dfRowWithAllVariations)

    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(vector.toSeq.toFloat === expected)
  }

  it should "abide by the rules of vectorization: ArrayType, null under key => feature is NaN" in {
    val gfs = GroupedFeatures(Seq(
      FeaturesGroup("d_array_values", Array("0", "1"), isArray = true),
      FeaturesGroup("f_array_values", Array("0", "1"), isArray = true)
    ))
    val expected: Seq[Float] = Seq(42.0f, Float.NaN, 42.0f, Float.NaN)

    val vector = extractVector(gfs, dfRowWithAllVariations)

    implicit val eq = implicits.seqFloatEqualsWithNaNZeroTolerance
    assert(vector.toSeq.toFloat === expected)
  }

}

object FeaturesRowDecoderTest {

  val nan: Float = Float.NaN

  object implicits {
    implicit val seqFloatEqualsWithNaNZeroTolerance: Equality[Seq[Float]] = createSeqFloatsEquality((a, b) => floatsWithNaNAreEqual(a, b, 0f))
  }

  val dfRowWithAllVariations: Row = {
    import org.apache.spark.sql.types._
    new GenericRowWithSchema(Array[Any](
      Map("s42" -> "42"),
      Map("f42" -> 42.0, "fnull" -> null), null,
      Map("f42" -> 42.0f, "fnull" -> null), null,
      mutable.WrappedArray.make(Array(42.0, null)), null,
      mutable.WrappedArray.make(Array(42.0f, null)), null
    ), StructType(Seq(
      StructField("s_map_field", MapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("d_map_values", MapType(DataTypes.StringType, DataTypes.DoubleType)),
      StructField("d_map_nullfield", MapType(DataTypes.StringType, DataTypes.DoubleType)),
      StructField("f_map_values", MapType(DataTypes.StringType, DataTypes.FloatType)),
      StructField("f_map_nullfield", MapType(DataTypes.StringType, DataTypes.FloatType)),
      StructField("d_array_values", ArrayType(DataTypes.DoubleType)),
      StructField("d_array_nullfield", ArrayType(DataTypes.DoubleType)),
      StructField("f_array_values", ArrayType(DataTypes.FloatType)),
      StructField("f_array_nullfield", ArrayType(DataTypes.FloatType))
    )))
  }

  def extractVector(gfs: GroupedFeatures, row: Row): Array[Double] = {
    val mergedGfs: GroupedFeatures = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))
    val allFeatures: Array[Double] = FeaturesRowDecoder(row.schema, mergedGfs).decode(row)
    val indices = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)

    GroupedFeatures.sliceFeaturesVector(allFeatures, indices)
  }

  def floatsWithNaNAreEqual(a: Float, b: Float, tolerance: Float): Boolean = {
    (a.isNaN && b.isNaN) ||
      ((a <= b + tolerance) && (a >= b - tolerance))
  }

  def input: Seq[GrinderDatasetRow] = Seq(
    GrinderDatasetRow(
      uid = "a",
      topics_motor200 = Some(Map("topic_1" -> 0.1f, "topic_2" -> 0.2f, "topic_3" -> 0.3f)),
      vk_groups_all = Some(Map("1" -> 1.0, "2" -> 2.0, "3" -> 3.0)),
      all_profs = Some(Array(0.99f, 0.98f, 0.97f, 0.96f, 0.95f))
    ))

  case class GrinderDatasetRow
  (
    uid: String,
    topics_motor200: Option[Map[String, Float]],
    vk_groups_all: Option[Map[String, Double]],
    all_profs: Option[Array[Float]]
  )

}
