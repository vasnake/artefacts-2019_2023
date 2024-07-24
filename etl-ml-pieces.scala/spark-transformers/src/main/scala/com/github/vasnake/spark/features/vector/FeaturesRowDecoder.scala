/**
 * Created by vasnake@gmail.com on 2024-07-24
 */
package com.github.vasnake.spark.features.vector

import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, StructType}
import org.apache.spark.sql.Row
import scala.collection.mutable
import com.github.vasnake.`etl-core`.GroupedFeatures

/**
  * Decoder for DataFrame collection fields (map, array); assemble features vector as Array[Double] from Row.
  *
  * Possible 'no data' cases:
  *  - DataFrame have no field $name: throw exception;
  *  - field value is null:
  *     - if field type is Map: all features = 0.0;
  *     - if field type is Array: all features = NaN;
  *  - field value is Map:
  *     - have not feature key: feature = 0;
  *     - feature value is null: feature = NaN;
  *  - field value is Array:
  *     - have not feature key: feature = NaN;
  *     - feature value is null: feature = NaN;
  */
case class FeaturesRowDecoder(outVectorLength: Int, columnsDecoders: Seq[CollectionColumnDecoder]) {
  def decode(row: Row): Array[Double] = {
    val vec = new Array[Double](outVectorLength)

    // mutations!
    var currIdx: Int = 0
    columnsDecoders.foreach(cd => {
      currIdx = cd.updateFeaturesVector(vec, row, currIdx)
    })

    vec
  }
}

object FeaturesRowDecoder {
  def apply(schema: StructType, fgroupsColl: GroupedFeatures): FeaturesRowDecoder = {
    val decoders: Seq[CollectionColumnDecoder] = fgroupsColl.groups.map(group =>
      CollectionColumnDecoder(group.name, schema, group.index)
    )

    FeaturesRowDecoder(fgroupsColl.featuresSize, decoders)
  }
}

trait CollectionColumnDecoder {

  def fieldIndex: Int
  def featuresIndices: Array[_]
  def featureValueIfFieldIsNull: Double
  protected def writeFeatures(from: Row, to: Array[Double], startIdx: Int): Int

  def updateFeaturesVector(vec: Array[Double], row: Row, startIdx: Int): Int = {
    if (row.isNullAt(fieldIndex)) {
      var idx: Int = startIdx
      featuresIndices.foreach(_ => {
        vec(idx) = featureValueIfFieldIsNull
        idx += 1
      })

      idx
    }
    else writeFeatures(row, vec, startIdx)
  }
}

object CollectionColumnDecoder {

  private val mapFieldDefaults = FieldDefaults(nullField = 0, noFeatureKey = 0, nullFeature = Double.NaN)
  private val arrayFieldDefaults = FieldDefaults(nullField = Double.NaN, noFeatureKey = Double.NaN, nullFeature = Double.NaN)

  def apply(colName: String, schema: StructType, featuresIndices: Array[String]): CollectionColumnDecoder = {
    val fieldIndex = schema.fieldIndex(colName)

    schema(fieldIndex).dataType match {

      case MapType(DataTypes.StringType, DataTypes.FloatType, _) => MapColumnDecoder[Float](fieldIndex, featuresIndices,
        mapFieldDefaults.nullField, mapFieldDefaults.noFeatureKey.toFloat, mapFieldDefaults.nullFeature.toFloat, x => x)

      case MapType(DataTypes.StringType, DataTypes.DoubleType, _) => MapColumnDecoder[Double](fieldIndex, featuresIndices,
        mapFieldDefaults.nullField, mapFieldDefaults.noFeatureKey, mapFieldDefaults.nullFeature, identity)

      case ArrayType(DataTypes.FloatType, _) => ArrayColumnDecoder[Float](fieldIndex, featuresIndices.map(_.toInt),
        arrayFieldDefaults.nullField, arrayFieldDefaults.noFeatureKey.toFloat, arrayFieldDefaults.nullFeature.toFloat, x => x)

      case ArrayType(DataTypes.DoubleType, _) => ArrayColumnDecoder[Double](fieldIndex, featuresIndices.map(_.toInt),
        arrayFieldDefaults.nullField, arrayFieldDefaults.noFeatureKey, arrayFieldDefaults.nullFeature, identity)

      case x => throw new IllegalArgumentException(
        s"Unknown type '$x' of collection column with name '${schema(fieldIndex).name}' in schema '$schema'")

    }
  }
}

case class MapColumnDecoder[@specialized(Float, Double) T]
(
  fieldIndex: Int,
  featuresIndices: Array[String],
  featureValueIfFieldIsNull: Double,
  featureValueIfNoKey: T,
  featureValueIfValueIsNull: T,
  toDouble: T => Double
) extends CollectionColumnDecoder {

  override protected def writeFeatures(row: Row, vec: Array[Double], startIdx: Int): Int = {
    val field = row.getAs[Map[String, Any]](fieldIndex)
    var idx: Int = startIdx
    var fv: Any = featureValueIfNoKey

    featuresIndices.foreach(fi => {
      vec(idx) = toDouble {
        fv = field.getOrElse(fi, featureValueIfNoKey)
        if (fv == null) featureValueIfValueIsNull
        else fv.asInstanceOf[T]
      }

      idx += 1
    })

    idx
  }
}

case class ArrayColumnDecoder[@specialized(Float, Double) T]
(
  fieldIndex: Int,
  featuresIndices: Array[Int],
  featureValueIfFieldIsNull: Double,
  featureValueIfNoKey: T,
  featureValueIfValueIsNull: T,
  toDouble: T => Double
) extends CollectionColumnDecoder {

  override protected def writeFeatures(row: Row, vec: Array[Double], startIdx: Int): Int = {
    val field = row.getAs[mutable.WrappedArray[Any]](fieldIndex)
    var idx: Int = startIdx

    featuresIndices.foreach(fi => {
      vec(idx) = toDouble {
        if (fi < 0 || fi >= field.length) featureValueIfNoKey
        else {
          if (field(fi) == null) featureValueIfValueIsNull
          else field(fi).asInstanceOf[T]
        }
      }

      idx += 1
    })

    idx
  }
}

case class FieldDefaults(nullField: Double, noFeatureKey: Double, nullFeature: Double)
