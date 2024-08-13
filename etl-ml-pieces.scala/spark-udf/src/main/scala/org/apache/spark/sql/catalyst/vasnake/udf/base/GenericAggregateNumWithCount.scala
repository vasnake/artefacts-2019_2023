/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf.base

import scala.util._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.vasnake.udf.accum.{ NumericAccumulatorWithCount => AccImpl, _ }
import org.apache.spark.sql.catalyst.vasnake.udf.codec._
import org.apache.spark.sql.types._

abstract class GenericAggregateNumWithCount()
    extends TypedImperativeAggregate[Accumulator]
       with ImplicitCastInputTypes
       with Logging {
  @inline protected def debug(msg: => String): Unit = {
    // logDebug(msg)
  }

  def child: Expression

  @inline def combineItems(x: AccImpl.V, y: AccImpl.V): AccImpl.V // ignore null or nan values, but if all null and nan, result is nan
  @inline def evalItem(x: AccImpl.V): AccImpl.VN

  override def serialize(buff: Accumulator): Array[Byte] = buff.serialize
  override def deserialize(bytes: Array[Byte]): Accumulator =
    createAggregationBuffer().deserialize(bytes)

  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = codec.resultType
  override def createAggregationBuffer(): Accumulator = AccImpl.apply()

  override def inputTypes: Seq[AbstractDataType] =
    // TODO: compute values from codec object and children seq
    children.map(_ => AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    Try {
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

  override def update(accum: Accumulator, input: InternalRow): Accumulator = {
    // update accumulator from row
    // debug(s"update enter: input: ${input}, buffer: ${accum}, child: ${child}")
    val exprValue: Any = child.eval(input)

    if (exprValue == null)
      debug("update: input is null, do nothing.")
    else if (isArray) {
      accum.containerIsNull(false)
      val arr: ArrayData = codec.decodeArray(exprValue)
      (0 until arr.numElements) foreach { i =>
        updateBuffer(accum, AccImpl.kvOps.decodeKey(i), parseInputElem(arr, i))
      }
    }
    else if (isMap) {
      accum.containerIsNull(false)
      val map: MapData = codec.decodeMap(exprValue)
      // debug(s"update: with map<$keyDataType, $valueDataType>, data size: ${map.numElements}")
      map.foreach(
        keyDataType,
        valueDataType,
        {
          case (k, v) =>
            updateBuffer(accum, AccImpl.kvOps.decodeKey(k), parseInputElem(map, v))
        },
      )
    }
    else // is primitive
    if (isInvalidInput(exprValue)) debug("update: input is invalid, do nothing.")
    else {
      accum.containerIsNull(false)
      updateBuffer(accum, AccImpl.PRIMITIVE_KEY, parseInputElem(exprValue))
    }

    // debug(s"update exit: buffer: ${accum}")
    accum
  }

  override def merge(accum: Accumulator, other: Accumulator): Accumulator = {
    debug(s"merge enter: buffer: ${accum}, other: ${other}")

    if (!other.containerIsNull) accum.containerIsNull(false)

    // TODO: refactor interfaces
    AccImpl.apply(other).foreach { case (key, value) => updateBuffer(accum, key, value) }

    debug(s"merge exit: buffer: ${accum}")
    accum
  }

  override def eval(accum: Accumulator): Any = {
    // return dataType object, spark sql ArrayData, MapData, InternalRow or primitive
    debug(s"eval enter: buffer: ${accum}")

    if (isArray) {
      debug(
        s"eval: array, buffer.isEmpty: ${accum.isEmpty}, containerIsNull: ${accum.containerIsNull}"
      )
      if (accum.containerIsNull) null else generateOutputArray(accum)
    }
    else if (isMap) {
      debug(
        s"eval: map, buffer.isEmpty: ${accum.isEmpty}, containerIsNull: ${accum.containerIsNull}"
      )
      if (accum.containerIsNull) null else generateOutputMap(accum)
    }
    else { // primitive
      debug(s"eval: primitive, buffer.isEmpty: ${accum.isEmpty}")
      if (accum.isEmpty) null else generateOutputPrimitive(accum)
    }
  }

  @inline protected def updateBuffer(
    accum: Accumulator,
    key: AccImpl.K,
    value: AccImpl.V,
  ): Unit =
    // If the key doesn't exist yet in the hash map, set its value to defaultValue;
    // otherwise, set its value to mergeValue(oldValue).
    AccImpl.changeValue(accum, key, defaultValue = value, mergeValue = x => combineItems(x, value))

  @inline protected def isInvalidInput(value: Any): Boolean =
    if (value == null) true
    else codec.isInvalidValue(value)

  @inline protected def parseInputElem(value: Any): AccImpl.V =
    AccImpl
      .kvOps
      .decodeValue(
        codec.decodeValue(value)
      )

  @inline protected def parseInputElem(map: MapData, value: Any): AccImpl.V =
    if (value == null) AccImpl.nullValue
    else if (codec.isInvalidValue(value)) AccImpl.nullValue
    else parseInputElem(value)

  @inline protected def parseInputElem(arr: ArrayData, i: Int): AccImpl.V =
    if (arr.isNullAt(i)) AccImpl.nullValue
    else {
      val value = arr.get(i, valueDataType)
      if (codec.isInvalidValue(value)) AccImpl.nullValue
      else parseInputElem(value)
    }

  protected def generateOutputPrimitive(accum: Accumulator): Any = {
    // not null input guaranteed
    // eval result, return dataType object, spark sql primitive
    debug(s"generateOutputPrimitive enter: accum: ${accum}")
    codec.encodeValue(
      evalItem(
        AccImpl.get(accum, AccImpl.PRIMITIVE_KEY)
      )
    )
  }

  protected def generateOutputArray(accum: Accumulator): Any =
    // not null input guaranteed, could be empty though or contain null|nan items
    // eval result, return dataType object, spark sql ArrayData, MapData, InternalRow or primitive
    generateOutputCollection(accum)

  protected def generateOutputMap(accum: Accumulator): Any =
    // not null input guaranteed, could be empty though or contain null|nan items
    // eval result, return dataType object, spark sql ArrayData, MapData, InternalRow or primitive
    generateOutputCollection(accum)

  private def generateOutputCollection(accum: Accumulator): Any = {
    debug(s"generateOutputCollection enter: accum: ${accum}")

    val evaluatedAccum: NumericAccumulator = NumericAccumulator()
    accum.asInstanceOf[AccImpl.A].foreach { case (k, v) => evaluatedAccum.update(k, evalItem(v)) }
    debug(s"generateOutputCollection, evaluated accum: ${evaluatedAccum}")

    val res = codec.encodeValue(evaluatedAccum)

    debug(s"generateOutputCollection exit, result ${res}")
    res
  }

  @transient private lazy val isArray: Boolean = codec.isArray
  @transient private lazy val isMap: Boolean = codec.isMap
  @transient private lazy val (keyDataType, valueDataType) = (codec.keyType, codec.valueType)

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
        s"GenericAggregateNumWithCount: unknown expression data type: ${child.dataType}"
      )
  }
}
