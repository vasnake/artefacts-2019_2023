/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf.accum

import java.io._
import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.vasnake.udf.accum.NumericAccumulator.serializationBufferSize
import org.apache.spark.sql.types._

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.OpenHashMap

class NumericAccumulator extends OpenHashMap[NumericAccumulator.K, NumericAccumulator.V] {
  import NumericAccumulator.{ K, V, SK, SV, decodeKey, encodeKey }

  // Array or map could be empty or null, it's two different cases;
  // empty buffer itself can't tell the difference.
  def containerIsNull(isnull: Boolean): Unit =
    update(null.asInstanceOf[K], if (isnull) 1.0 else -1.0)

  def containerIsNull: Boolean =
    if (contains(null.asInstanceOf[K])) apply(null.asInstanceOf[K]) > 0
    else true

  override def size: Int = itemsIterator.size // w/o null key
  // if (haveNullValue) _keySet.size + 1 else _keySet.size

  def itemsIterator: Iterator[(K, V)] = {
    val _iter = this.iterator

    new Iterator[(K, V)] {
      def hasNext: Boolean = nextPair != null

      def next(): (K, V) = {
        val pair = nextPair
        nextPair = computeNextPair()
        pair
      }

      private var nextPair: (K, V) = computeNextPair()

      @tailrec private def computeNextPair(): (K, V) =
        if (_iter.hasNext) {
          val p = _iter.next()
          if (p._1 == null) computeNextPair() else p
        }
        else null
    }
  }

  def serialize: Array[Byte] = {
    val itemBuff = new Array[Byte](serializationBufferSize)
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)
    try {
      val projection = UnsafeProjection.create(Array[DataType](SK, SV))
      // Write pairs in counts map to byte buffer
      this.foreach {
        case (k, v) =>
          val row = InternalRow.apply(encodeKey(k), v)
          val unsafeRow = projection.apply(row)
          out.writeInt(unsafeRow.getSizeInBytes)
          unsafeRow.writeToStream(out, itemBuff)
      }
      out.writeInt(-1)
      out.flush()

      bos.toByteArray
    }
    finally {
      out.close()
      bos.close()
    }
  }

  def deserialize(bytes: Array[Byte]): NumericAccumulator = {
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)
    try {
      val result = new NumericAccumulator
      // Read unsafeRow size and content in bytes.
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(2)
        row.pointTo(bs, sizeOfNextRow)
        // Insert the pairs into counts map.
        val key = decodeKey(row.get(0, SK))
        val count = row.get(1, SV).asInstanceOf[V]
        result.update(if (key == null) null else key.toString, count)
        sizeOfNextRow = ins.readInt()
      }

      result
    }
    finally {
      ins.close()
      bis.close()
    }
  }
}

object NumericAccumulator {
  type K = java.lang.String
  type V = java.lang.Double

  val PRIMITIVE_KEY: K = "1"
  val NULL_VALUE: V = null.asInstanceOf[V]

  private val SK = StringType
  private val SV = DoubleType

  // The right way to make it configurable is: pass a serializable 'config' object all the way down from the udf registration point.
  // On the time of registration get config from the SparkConf obj.
  // see KryoSerializer https://github.com/apache/spark/blob/3065dd92ab8f36b019c7be06da59d47c1865fe60/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L62C1-L62C38
  val serializationBufferSize: Int = 32 << 10 // 32Kb buffer for off-heap serialization

  private def decodeKey(key: Any): Any = key.asInstanceOf[UTF8String]
  private def encodeKey(key: K): Any = UTF8String.fromString(key)

  def apply(): NumericAccumulator = new NumericAccumulator()

  def deserialize(bytes: Array[Byte]): NumericAccumulator = apply().deserialize(bytes)

  def keyConverter(targetType: DataType): (Any => Any) = targetType match {
    case StringType => k: Any => UTF8String.fromString(k.asInstanceOf[K])
    case FloatType => k: Any => k.asInstanceOf[K].toFloat
    case DoubleType => k: Any => k.asInstanceOf[K].toDouble
    case IntegerType => k: Any => k.asInstanceOf[K].toInt
    case ByteType => k: Any => k.asInstanceOf[K].toByte
    case LongType => k: Any => k.asInstanceOf[K].toLong
    case ShortType => k: Any => k.asInstanceOf[K].toShort
    case BooleanType => k: Any => k.asInstanceOf[K].toBoolean
    case DateType => k: Any => k.asInstanceOf[K].toInt
    case TimestampType => k: Any => k.asInstanceOf[K].toLong
    case _ => throw new IllegalArgumentException(s"Accumulator: unknown key type: `${targetType}`")
  }

  def valConverter(targetType: DataType): (Any => Any) = targetType match {
    case FloatType => v: Any => if (v == null) null else v.asInstanceOf[V].floatValue()
    case DoubleType => v: Any => if (v == null) null else v.asInstanceOf[V]
    case IntegerType => v: Any => if (v == null) null else v.asInstanceOf[V].intValue()
    case ByteType => v: Any => if (v == null) null else v.asInstanceOf[V].byteValue()
    case LongType => v: Any => if (v == null) null else v.asInstanceOf[V].longValue()
    case ShortType => v: Any => if (v == null) null else v.asInstanceOf[V].shortValue()
    case DecimalType.Fixed(precision, scale) =>
      v: Any => if (v == null) null else Decimal(BigDecimal(v.asInstanceOf[V]), precision, scale)
    case _ =>
      throw new IllegalArgumentException(s"Accumulator: unknown value type: `${targetType}`")
  }

  def rowToBufferValueConverter(srcType: DataType): Any => V = srcType match {
    case DecimalType.Fixed(_, _) =>
      v: Any => if (v == null) null else v.asInstanceOf[Decimal].toDouble
    case _ => v: Any => if (v == null) null else v.asInstanceOf[java.lang.Number].doubleValue()
  }

  @inline def keyValue(k: Any): K = k.toString
}
