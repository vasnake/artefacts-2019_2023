/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf.accum

import java.io._
import java.lang.{ Double => jDouble, Long => jLong }

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class NumericAccumulatorWithCount[K, V] extends Accumulator with Iterable[(K, V)] {
  import org.apache.spark.sql.catalyst.vasnake.udf.accum.{ NumericAccumulatorWithCount => CO }

  override def toString(): String = s"NumericAccumulatorWithCount{accum = ${accum}"

  private val accum = mutable.Map.empty[K, V] // new OpenHashMap[K, V]() // import org.apache.spark.util.collection.OpenHashMap
  implicit val keyValueOps: MapKVOps[K, V] = CO.kvOps.asInstanceOf[MapKVOps[K, V]]

  override def iterator: Iterator[(K, V)] = itemsIterator

  override def serialize: Array[Byte] = _serialize

  override def deserialize(bytes: Array[Byte]): Accumulator = CO.deserialize(bytes)(CO.kvOps)

  def _serialize(
    implicit
    kv: MapKVOps[K, V]
  ): Array[Byte] = {
    val itemBuff = new Array[Byte](4 << 10) // 4K item size, TODO: make it a configurable parameter
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)

    try {
      val projection = UnsafeProjection.create(kv.sqlTypes)
      // Write kv pairs to byte buffer
      accum.foreach {
        case (k, v) =>
          val row = kv.internalRow(k, v)
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

  def itemsIterator: Iterator[(K, V)] = {
    val _iter = accum.iterator

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

  def containerIsNull(isnull: Boolean): Unit =
    accum.update(
      null.asInstanceOf[K],
      if (isnull) keyValueOps.positiveValue else keyValueOps.negativeValue,
    )

  def containerIsNull: Boolean =
    if (accum.contains(null.asInstanceOf[K]))
      keyValueOps.isValuePositive(accum(null.asInstanceOf[K]))
    else true

  def changeValue(
    k: K,
    defaultValue: => V,
    mergeValue: (V) => V,
  ): V = {
    // If the key doesn't exist yet in the hash map, set its value to defaultValue;
    // otherwise, set its value to mergeValue(oldValue).
    val v = accum.get(k).map(v => mergeValue(v)).getOrElse(defaultValue)
    accum.update(k, v)
    v
  }

  def get(k: K): Option[V] = accum.get(k)
}

object NumericAccumulatorWithCount {
  type K = java.lang.String
  type V = AccumulatorValue
  type VC = jLong
  type VN = jDouble
  type A = NumericAccumulatorWithCount[K, V]
  val PRIMITIVE_KEY: String = "1"

  def apply(): NumericAccumulatorWithCount[K, V] = new NumericAccumulatorWithCount[K, V]()
  def apply(accum: Accumulator): NumericAccumulatorWithCount[K, V] =
    accum.asInstanceOf[NumericAccumulatorWithCount[K, V]]

  def deserialize(
    bytes: Array[Byte]
  )(implicit
    kv: MapKVOps[K, V]
  ): A = {
    val result = apply()
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(bis)
    try {
      var sizeOfNextRow = ins.readInt()
      while (sizeOfNextRow >= 0) {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(kv.numFields)
        row.pointTo(bs, sizeOfNextRow)
        result.accum.update(kv.getKey(row), kv.getValue(row))
        sizeOfNextRow = ins.readInt()
      }

      result
    }
    finally {
      ins.close()
      bis.close()
    }
  }

  def changeValue(
    accum: Accumulator,
    k: K,
    defaultValue: => V,
    mergeValue: (V) => V,
  ): V =
    // If the key doesn't exist yet in the hash map, set its value to defaultValue;
    // otherwise, set its value to mergeValue(oldValue).
    accum.asInstanceOf[A].changeValue(k, defaultValue, mergeValue)

  def get(accum: Accumulator, k: K): V =
    accum.asInstanceOf[A].accum.getOrElse(k, sys.error(s"Key `${k}` doesn't exists in accumulator"))

  def nullValue: V = AccumulatorValue(1, null.asInstanceOf[VN])

  val kvOps: MapKVOps[K, V] = new MapKVOps[K, V] {
    override def makeValue[A, B](a: A, b: B): V =
      AccumulatorValue(a.asInstanceOf[VC], b.asInstanceOf[VN])
    override def positiveValue: V = AccumulatorValue(1, 1.0)
    override def negativeValue: V = AccumulatorValue(1, -1.0)
    override def isValuePositive(v: V): Boolean = v.value > 0
    override def sqlTypes: Array[DataType] = Array[DataType](StringType, LongType, DoubleType)

    override def internalRow(k: K, v: V): InternalRow = {
      assume(v != null, "Require not null values")
      InternalRow.apply(UTF8String.fromString(k), v.count, v.value)
    }

    override def numFields: Int = 3

    override def getKey(row: UnsafeRow): K = {
      val k = row.get(0, StringType).asInstanceOf[UTF8String]
      if (k == null) null
      else k.toString
    }

    override def getValue(row: UnsafeRow): V =
      AccumulatorValue(
        row.get(1, LongType).asInstanceOf[VC],
        row.get(2, DoubleType).asInstanceOf[VN],
      )

    override def decodeKey(k: Any): K = k.toString
    override def decodeValue(v: Any): V = AccumulatorValue(1, v.asInstanceOf[VN])
  }
}

trait MapKVOps[K, V] { // map pair operations type class
  def makeValue[A, B](a: A, b: B): V
  def positiveValue: V // 1
  def negativeValue: V // -1
  def isValuePositive(v: V): Boolean // > 0
  def sqlTypes: Array[DataType] // Array[DataType](StringType, DoubleType)
  def internalRow(k: K, v: V): InternalRow // InternalRow.apply(UTF8String.fromString(k), v)
  def numFields: Int // 2
  def getKey(row: UnsafeRow): K // row.get(0, SK).asInstanceOf[UTF8String].toString // if not null, else null
  def getValue(row: UnsafeRow): V // row.get(1, SV).asInstanceOf[V]
  def decodeKey(k: Any): K // = k.toString
  def decodeValue(v: Any): V // v.asInstanceOf[Number].doubleValue()
}

case class AccumulatorValue(
  count: NumericAccumulatorWithCount.VC,
  value: NumericAccumulatorWithCount.VN,
)

trait Accumulator {
  def serialize: Array[Byte]
  def deserialize(bytes: Array[Byte]): Accumulator

  /** Set `container is null` property, for "container" columns like map or array.
    * @param isnull container is null if it's true
    */
  def containerIsNull(isnull: Boolean): Unit

  /** Get `container is null` property, for "container" columns like map or array.
    * @return true if container is null, false otherwise
    */
  def containerIsNull: Boolean

  def isEmpty: Boolean
}
