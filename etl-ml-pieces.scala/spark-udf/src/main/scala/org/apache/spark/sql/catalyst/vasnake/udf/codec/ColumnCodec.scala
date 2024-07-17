/**
 * Created by vasnake@gmail.com on 2024-07-17
 */
package org.apache.spark.sql.catalyst.vasnake.udf.codec

import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap}

import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.vasnake.udf.accum.NumericAccumulator

trait ColumnCodec {
  def resultType: sql.types.DataType

  def valueType: sql.types.DataType = resultType
  def keyType: sql.types.DataType = sql.types.NullType
  def isArray: Boolean = false
  def isMap: Boolean = false

  // TODO: make it more generic, I'm not happy with current state
  // NumericAccumulator.V|K must be isolated

  // sql => accum
  def isInvalidValue(value: Any): Boolean = {
    assume(value != null, "NULL value should be handled upstream")
    false // valid by default, float and double override this default
  }
  def decodeValue(value: Any): NumericAccumulator.V = value.asInstanceOf[Number].doubleValue()
  def decodeKey(key: Any): NumericAccumulator.K = key.toString

  // accum => sql
  def encodeValue(value: String): Any = UTF8String.fromString(value)
  def encodeValue(value: NumericAccumulator.V): Any
  def encodeValue(buffer: NumericAccumulator, key: NumericAccumulator.K = NumericAccumulator.PRIMITIVE_KEY): Any = encodeValue(buffer(key))

  def decodeMap(m: Any): MapData = m match {
    case a: UnsafeMapData => a
    case b: ArrayBasedMapData => b
    case _ => m.asInstanceOf[ColumnarMap]
  }

  def decodeArray(a: Any): ArrayData = a match {
    case x: UnsafeArrayData => x
    case y: GenericArrayData => y
    case _ => a.asInstanceOf[ColumnarArray]
  }
}

case class BooleanColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.BooleanType
  override def decodeValue(value: Any): NumericAccumulator.V = if (!value.asInstanceOf[java.lang.Boolean]) 0.0 else 1.0
  def encodeValue(value: NumericAccumulator.V): Any = value.byteValue() != 0
  override def encodeValue(value: String): Any = !(value == "0.0" || value == "false")
}

case class StringColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.StringType
  override def decodeKey(key: Any): NumericAccumulator.K = key.asInstanceOf[UTF8String].toString
  def encodeValue(value: NumericAccumulator.V): Any = UTF8String.fromString(value.toString)
}

case class IntegerColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.IntegerType
  def encodeValue(value: NumericAccumulator.V): Any = value.intValue()
  override def encodeValue(value: String): Any = value.toInt
}

case class LongColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.LongType
  def encodeValue(value: NumericAccumulator.V): Any = value.longValue()
  override def encodeValue(value: String): Any = value.toLong
}

case class ByteColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.ByteType
  def encodeValue(value: NumericAccumulator.V): Any = value.byteValue()
  override def encodeValue(value: String): Any = value.toByte
}

case class ShortColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.ShortType
  def encodeValue(value: NumericAccumulator.V): Any = value.shortValue()
  override def encodeValue(value: String): Any = value.toShort
}

case class FloatColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.FloatType

  override def isInvalidValue(value: Any): Boolean = {
    val v = decodeValue(value)
    v.isNaN || v.isInfinite
  }

  def encodeValue(value: NumericAccumulator.V): Any = value.floatValue()
  override def encodeValue(value: String): Any = value.toFloat
}

case class DoubleColumnCodec() extends ColumnCodec {
  val resultType: DataType = sql.types.DoubleType

  override def isInvalidValue(value: Any): Boolean = {
    val v = decodeValue(value)
    v.isNaN || v.isInfinite
  }

  def encodeValue(value: NumericAccumulator.V): Any = value
  override def encodeValue(value: String): Any = value.toDouble
}

case class DecimalColumnCodec(precision: Int, scale: Int) extends ColumnCodec {
  val resultType: DataType = sql.types.DecimalType.bounded(precision + 10, scale)
  override def decodeValue(value: Any): NumericAccumulator.V = value.asInstanceOf[Decimal].toDouble
  def encodeValue(value: NumericAccumulator.V): Any = Decimal(BigDecimal(value), precision + 10, scale)
  override def encodeValue(value: String): Any = Decimal(BigDecimal(value), precision + 10, scale)
}

case class MapColumnCodec(override val keyType: DataType, override val valueType: DataType) extends ColumnCodec {
  // value type
  if (!Seq(
    FloatType, DoubleType, IntegerType, ByteType, LongType, ShortType, DecimalType
  ).exists(_.acceptsType(valueType)))
    throw new IllegalArgumentException(s"Map codec: unknown value type: `${valueType}`")

  // key type
  if (!Seq(
    FloatType, DoubleType, IntegerType, ByteType, LongType, ShortType, BooleanType, DateType, TimestampType, StringType
  ).exists(_.acceptsType(keyType)))
    throw new IllegalArgumentException(s"Map codec: unknown key type: `${keyType}`")

  val resultType: DataType = sql.types.MapType(keyType, valueType, valueContainsNull = true)
  override def isArray: Boolean = false
  override def isMap: Boolean = true

  // buffer to row
  private val keyConverter: Any => Any = NumericAccumulator.keyConverter(keyType)
  private val valConverter: Any => Any = NumericAccumulator.valConverter(valueType)
  // row to buffer
  private val row2bufValueConverter: Any => NumericAccumulator.V = NumericAccumulator.rowToBufferValueConverter(valueType)

  override def isInvalidValue(value: Any): Boolean = {
    val x = row2bufValueConverter(value)
    x.isNaN || x.isInfinite
  }

  override def decodeValue(value: Any): NumericAccumulator.V = row2bufValueConverter(value)

  override def encodeValue(buffer: NumericAccumulator, key: NumericAccumulator.K = NumericAccumulator.PRIMITIVE_KEY): Any = {
    ArrayBasedMapData(
      buffer.itemsIterator,
      size = buffer.size,
      keyConverter = keyConverter,
      valueConverter = valConverter
    )
  }

  def encodeValue(value: NumericAccumulator.V): Any = ???

}

case class ArrayColumnCodec(override val valueType: DataType) extends ColumnCodec {
  if (!Seq(
    FloatType, DoubleType, IntegerType, ByteType, LongType, ShortType, DecimalType
  ).exists(_.acceptsType(valueType)))
    throw new IllegalArgumentException(s"Array codec: unknown value type: `${valueType}`")

  val resultType: DataType = sql.types.ArrayType(valueType, containsNull = true)
  override def isArray: Boolean = true
  override def isMap: Boolean = false

  private val codec: ArrayValueCodec = valueType match {
    case FloatType => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[java.lang.Float](size)
      override def encode(v: NumericAccumulator.V): Any = if (v == null) null else v.floatValue()
    }
    case DoubleType => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[java.lang.Double](size)
      override def encode(v: NumericAccumulator.V): Any = v
    }
    case IntegerType => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[java.lang.Integer](size)
      override def encode(v: NumericAccumulator.V): Any = if (v == null) null else v.intValue()
    }
    case ByteType => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[java.lang.Byte](size)
      override def encode(v: NumericAccumulator.V): Any = if (v == null) null else v.byteValue()
    }
    case LongType => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[java.lang.Long](size)
      override def encode(v: NumericAccumulator.V): Any = if (v == null) null else v.longValue()
    }
    case ShortType => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[java.lang.Short](size)
      override def encode(v: NumericAccumulator.V): Any = if (v == null) null else v.shortValue()
    }
    case DecimalType.Fixed(precision, scale) => new ArrayValueCodec {
      override def array(size: Int): Array[Any] = _array[Decimal](size)
      override def encode(v: NumericAccumulator.V): Any = if (v == null) null else Decimal(BigDecimal(v), precision, scale)
      override def decode(v: Any): NumericAccumulator.V = v.asInstanceOf[Decimal].toDouble
    }
    case _ => throw new IllegalArgumentException(s"Array codec, unknown value type: `${valueType}`")
  }

  override def isInvalidValue(value: Any): Boolean =
    codec.isInvalid(value)

  override def encodeValue(buffer: NumericAccumulator, key: NumericAccumulator.K): Any = {
    val arr: Array[Any] = codec.array(buffer.size)
    arr.indices.foreach { i => arr.update(i, codec.encode(buffer(NumericAccumulator.keyValue(i)))) }

    ArrayData.toArrayData(arr)
  }

  override def decodeValue(value: Any): NumericAccumulator.V =
    codec.decode(value)

  def encodeValue(value: NumericAccumulator.V): Any = ???

}

trait ArrayValueCodec {
  def array(size: Int): Array[Any]
  def encode(v: NumericAccumulator.V): Any
  def decode(v: Any): NumericAccumulator.V = v.asInstanceOf[Number].doubleValue()
  def isInvalid(v: Any): Boolean = {
    val x = decode(v)
    x.isNaN || x.isInfinite
  }
  protected def _array[T: ClassTag](size: Int): Array[Any] = new Array[T](size).asInstanceOf[Array[Any]]
}
