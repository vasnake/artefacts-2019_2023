/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf.base

import scala.util._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.vasnake.udf.accum.NumericAccumulator
import org.apache.spark.sql.catalyst.vasnake.udf.codec._

// There are two GenericAggregate..., TODO: DRY
abstract class GenericAggregate()
    extends TypedImperativeAggregate[NumericAccumulator]
       with ImplicitCastInputTypes
       with Logging {
  // imperative agg uses: InternalRow, buffer, update, merge, eval, ser/des

  // spec and examples:
  // abstract class ImperativeAggregate extends AggregateFunction with CodegenFallback { ... }
  // import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
  // abstract class TypedImperativeAggregate[T] extends ImperativeAggregate { ... }
  // import org.apache.spark.sql.catalyst.expressions.aggregate.Percentile
  // import org.apache.spark.sql.hive.HiveUDAFFunction

  @inline protected def debug(msg: => String): Unit = {}

  def child: Expression

  @inline def combineItems(x: NumericAccumulator.V, y: NumericAccumulator.V): NumericAccumulator.V

  override def serialize(buff: NumericAccumulator): Array[Byte] = buff.serialize
  override def deserialize(bytes: Array[Byte]): NumericAccumulator = NumericAccumulator.deserialize(bytes)
  override def createAggregationBuffer(): NumericAccumulator = new NumericAccumulator()

  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = codec.resultType
  override def inputTypes: Seq[AbstractDataType] = children.map(_ => AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult = Try {
    dataType.isInstanceOf[DecimalType] || dataType.acceptsType(child.dataType)
  } match {
    case Success(ok) =>
      if (ok) TypeCheckResult.TypeCheckSuccess
      else
        TypeCheckResult.TypeCheckFailure(
          s"function $prettyName, unknown type: ${child.dataType.catalogString}"
        )
    case Failure(err) =>
      TypeCheckResult.TypeCheckFailure(
        s"function $prettyName, input type mismatch: ${err.getMessage}"
      )
  }

  override def update(buffer: NumericAccumulator, input: InternalRow): NumericAccumulator = {
    val exprValue: Any = child.eval(input)

    if (exprValue == null)
      debug("update: input is null, do nothing.")
    else if (isArray) {
      buffer.containerIsNull(false)
      val arr: ArrayData = codec.decodeArray(exprValue)
      (0 until arr.numElements) foreach { i =>
        updateBuffer(buffer, NumericAccumulator.keyValue(i), parseInputElem(arr, i))
      }
    }
    else if (isMap) {
      buffer.containerIsNull(false)
      val map: MapData = codec.decodeMap(exprValue)
      map.foreach(
        keyDataType,
        valueDataType,
        {
          case (k, v) =>
            updateBuffer(buffer, NumericAccumulator.keyValue(k), parseInputElem(map, v))
        }
      )
    }
    else // primitive
    if (isInvalidInput(exprValue)) debug("update: input is invalid, do nothing.")
    else {
      buffer.containerIsNull(false)
      updateBuffer(buffer, NumericAccumulator.PRIMITIVE_KEY, parseInputElem(exprValue))
    }

    buffer
  }

  override def eval(buffer: NumericAccumulator): Any =
    // must return dataType object, spark sql ArrayData, MapData, InternalRow or primitive
    // logDebug(s"eval enter: buffer: ${buffer}")
    if (isArray)
      if (buffer.containerIsNull) null else generateOutputArray(buffer)
    else if (isMap)
      if (buffer.containerIsNull) null else generateOutputMap(buffer)
    else // primitive
    if (buffer.isEmpty) null
    else generateOutputPrimitive(buffer)

  override def merge(buffer: NumericAccumulator, other: NumericAccumulator): NumericAccumulator = {
    // debug(s"merge enter: buffer: ${buffer}, other: ${other}")

    if (!other.containerIsNull) buffer.containerIsNull(false)

    other.foreach { case (key, value) => updateBuffer(buffer, key, value) }

    // debug(s"merge exit: buffer: ${buffer}")
    buffer
  }

  @inline protected def updateBuffer(
    buffer: NumericAccumulator,
    key: NumericAccumulator.K,
    value: NumericAccumulator.V
  ): Unit =
    buffer.changeValue(key, value, combineItems(_, value))

  @transient protected lazy val codec: ColumnCodec = child.dataType match {
    case FloatType => FloatColumnCodec()
    case DoubleType => DoubleColumnCodec()
    case IntegerType => IntegerColumnCodec()
    case LongType => LongColumnCodec()
    case ByteType => ByteColumnCodec()
    case ShortType => ShortColumnCodec()
    case DecimalType.Fixed(precision, scale) => DecimalColumnCodec(precision, scale)
    case ArrayType(valueType, _) => ArrayColumnCodec(valueType)
    case MapType(keyType, valueType, _) => MapColumnCodec(keyType, valueType)
    case _ =>
      throw new IllegalArgumentException(
        s"GenericAggregate: unknown expression data type: ${child.dataType}"
      )
  }

  @transient private lazy val isArray: Boolean = codec.isArray
  @transient private lazy val isMap: Boolean = codec.isMap
  @transient private lazy val (keyDataType, valueDataType) = (codec.keyType, codec.valueType)

  @inline protected def isInvalidInput(value: Any): Boolean =
    if (value == null) true
    else codec.isInvalidValue(value)

  @inline protected def parseInputElem(value: Any): NumericAccumulator.V = codec.decodeValue(value)

  @inline protected def parseInputElem(map: MapData, value: Any): NumericAccumulator.V =
    if (value == null) NumericAccumulator.NULL_VALUE
    else if (codec.isInvalidValue(value)) NumericAccumulator.NULL_VALUE
    else parseInputElem(value)

  @inline protected def parseInputElem(arr: ArrayData, i: Int): NumericAccumulator.V =
    if (arr.isNullAt(i)) NumericAccumulator.NULL_VALUE
    else {
      val value = arr.get(i, valueDataType)
      if (codec.isInvalidValue(value)) NumericAccumulator.NULL_VALUE
      else parseInputElem(value)
    }

  @inline protected def generateOutputPrimitive(buffer: NumericAccumulator): Any =
    codec.encodeValue(buffer, NumericAccumulator.PRIMITIVE_KEY)

  @inline protected def generateOutputArray(buffer: NumericAccumulator): Any =
    codec.encodeValue(buffer)

  @inline protected def generateOutputMap(buffer: NumericAccumulator): Any =
    codec.encodeValue(buffer)
}
