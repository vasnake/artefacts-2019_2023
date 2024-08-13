/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import scala.util._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.vasnake.udf.accum.{ NumericAccumulatorWithCount => AccImpl, _ }
import org.apache.spark.sql.catalyst.vasnake.udf.codec._
import org.apache.spark.sql.types._

@ExpressionDescription(usage = """_FUNC_(expr) - Returns the most frequent item in the group.""")
case class GenericMostFreq(
  child: Expression,
  indexExpression: Expression,
  thresholdExpression: Expression,
  preferExpression: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0,
) extends GenericCountItems {
  // reference: org.apache.spark.sql.catalyst.expressions.aggregate.Percentile
  def this(child: Expression) =
    this(child, Literal(null), Literal(null), Literal(null), 0, 0) // n.b. wanted by registry

  def this(
    child: Expression,
    indexExpression: Expression,
    thresholdExpression: Expression,
    preferExpression: Expression,
  ) =
    this(child, indexExpression, thresholdExpression, preferExpression, 0, 0)

  override def prettyName: String = "generic_most_freq"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] =
    child :: indexExpression :: thresholdExpression :: preferExpression :: Nil // TODO: inputTypes should reflect children types

  @inline def combineItems(x: AccImpl.V, y: AccImpl.V): AccImpl.V =
    if (x == null) y
    else if (y == null) x
    else {
      assume(x.count != null && y.count != null, "Require not null counts")
      AccImpl.kvOps.makeValue(x.count + y.count, 0.0)
    }

  @transient private lazy val randomGen = new scala.util.Random() // new scala.util.Random(7333L)

  @inline def evalItem(a: Accumulator): AccImpl.K = {
    debug(s"evalItem enter, accum: ${a}")
    // if prefer and prefer in accum: return prefer
    // if threshold: total = sum(accum.counts); accum = accum.filter(count / total >= threshold)
    // if accum is empty: return null
    // accum.selectTopItem(index): find list of top items; calc idx(index, top.size); return top(idx)

    val accum = AccImpl.apply(a)

    def makeIndex(bound: Int): Int = index.getOrElse(randomGen.nextInt(bound)) % bound

    def filterSortSelect(items: Array[(AccImpl.K, AccImpl.VC)]): AccImpl.K = {

      val filtered = threshold
        .map { td =>
          val total: Double = items.map(x => x._2.toLong).sum.toDouble
          assume(
            total > 0 || items.isEmpty,
            "Require sum of counts > 0 for non empty list of items",
          )

          items.filter { case (_, count) => count.toDouble / total >= td }
        }
        .getOrElse(items)

      if (filtered.isEmpty) null
      else {
        val sorted = filtered.sortWith {
          case (a, b) =>
            !(a._2 < b._2 || (a._2 == b._2 && a._1 < b._1)) // reverse
        }
        val topSize = sorted.count(_._2 == sorted(0)._2)
        if (topSize <= 1) sorted(0)._1
        else sorted(makeIndex(topSize))._1
      }
    }

    // proceed

    val preferredResult = for {
      p <- prefer
      if accum.get(p).isDefined
    } yield p

    preferredResult.getOrElse(
      filterSortSelect(
        accum.map { case (k, v) => (k, v.count) }.toArray
      )
    )

  }

  // N.B. in docs these parameters described as constants, not dynamic expressions

  @transient private lazy val prefer: Option[AccImpl.K] = {
    debug(s"prefer, expr ${preferExpression}")
    Option(preferExpression.eval()).map(p => p.toString)
  }

  @transient private lazy val threshold: Option[Double] = {
    debug(s"threshold, expr ${thresholdExpression}")
    Option(thresholdExpression.eval()).map(t => t.toString.toDouble)
  }

  @transient private lazy val index: Option[Int] = {
    debug(s"index, expr ${indexExpression}")
    Option(indexExpression.eval()).map(i => i.toString.toInt)
  }
}

// TODO: accumulator should be generic; actual accum. type should be derived from codec

abstract class GenericCountItems()
    extends TypedImperativeAggregate[Accumulator]
       with ImplicitCastInputTypes
       with Logging {
  @inline protected def debug(msg: => String): Unit = {}

  def child: Expression
  @inline def combineItems(x: AccImpl.V, y: AccImpl.V): AccImpl.V
  @inline def evalItem(a: Accumulator): AccImpl.K

  override def nullable: Boolean = true
  override def children: Seq[Expression] = child :: Nil
  override def serialize(buff: Accumulator): Array[Byte] = buff.serialize
  override def deserialize(bytes: Array[Byte]): Accumulator =
    AccImpl.deserialize(bytes)(AccImpl.kvOps)
  override def createAggregationBuffer(): Accumulator = AccImpl.apply()

  override def dataType: DataType = codec.resultType

  override def inputTypes: Seq[AbstractDataType] =
    // TODO: compute values from codec object and children seq
    children.map(_ => AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    // TODO: add tests for unsupported data types
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
    val exprValue: Any = child.eval(input)

    if (isInvalidInput(exprValue)) debug("update: input is invalid, do nothing.")
    else updateBuffer(accum, parseInputElem(exprValue), AccImpl.kvOps.decodeValue(0.0))

    accum
  }

  override def merge(accum: Accumulator, other: Accumulator): Accumulator = {
    debug(s"merge enter: buffer: ${accum}, other: ${other}")

    // TODO: refactor Accumulator interface
    AccImpl.apply(other).foreach { case (key, value) => updateBuffer(accum, key, value) }

    debug(s"merge exit: buffer: ${accum}")
    accum
  }

  override def eval(accum: Accumulator): Any = {
    // return dataType object, spark sql ArrayData, MapData, InternalRow or primitive
    debug(s"eval enter: buffer: ${accum}, isEmpty: ${accum.isEmpty}")

    if (accum.isEmpty) null else generateOutputPrimitive(accum)
  }

  @inline protected def updateBuffer(
    accum: Accumulator,
    key: AccImpl.K,
    value: AccImpl.V,
  ): Unit =
    // debug(s"updateBuffer enter, key `${key}`, value `${value}`, accum: ${accum}")

    // If the key doesn't exist yet in the hash map, add key and set its value to defaultValue;
    // otherwise, set its value to mergeValue(oldValue).
    AccImpl.changeValue(accum, key, defaultValue = value, mergeValue = x => combineItems(x, value))

    // debug(s"updateBuffer exit, accum ${accum}")

  @inline protected def isInvalidInput(value: Any): Boolean =
    if (value == null) true
    else codec.isInvalidValue(value)

  @inline protected def parseInputElem(value: Any): AccImpl.K =
    // expression value => accum value
    // debug(s"parseInputElem enter, value: `${value}` of type ${value.getClass.getSimpleName}")
    codec.decodeKey(value)

  protected def generateOutputPrimitive(accum: Accumulator): Any = {
    // not-null input guaranteed
    // eval result, return dataType object, spark sql primitive
    debug(s"generateOutputPrimitive enter: accum: ${accum}")

    val res = evalItem(accum)
    if (res == null) res
    else codec.encodeValue(res)
  }

  @transient protected lazy val codec: ColumnCodec = {
    debug(s"codec init enter, child: ${child} ${child.dataType}, ${child.sql}")

    val res = child.dataType match {
      case StringType => StringColumnCodec()
      case IntegerType => IntegerColumnCodec()
      case LongType => LongColumnCodec()
      case ShortType => ShortColumnCodec()
      case ByteType => ByteColumnCodec()
      case BooleanType => BooleanColumnCodec()
      case FloatType => FloatColumnCodec()
      case DoubleType => DoubleColumnCodec()
      case DecimalType.Fixed(precision, scale) => DecimalColumnCodec(precision, scale)
      // TODO: possible additions: DateType, TimestampType
      case _ =>
        throw new IllegalArgumentException(
          s"GenericCountItems: unknown expression data type: ${child.dataType}"
        )
    }

    debug(s"codec init exit, codec: ${res}")
    res
  }
}
