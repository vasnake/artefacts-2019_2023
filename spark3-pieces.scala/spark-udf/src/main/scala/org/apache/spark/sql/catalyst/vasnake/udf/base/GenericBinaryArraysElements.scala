/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf.base

import java.lang.{ Double => jDouble, Float => jFloat }

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._

trait GenericBinaryArraysElements extends ArrayBinaryLike with ComplexTypeMergingExpression {
  def binaryOp(x1: jDouble, x2: jDouble): jDouble

  override def nullable: Boolean = true // Can't say "it's not null" before looking at actual data

  override def dataType: DataType = {
    dataTypeCheck // Assert that both arguments have the same type
    // Case with mixed types forbidden, e.g. first argument Array[Int] and second Array[Float] => AnalysisException

    ArrayType(
      elementType, // Lazy value, computed from first argument
      containsNull = true // null, inf or nan element produces null element.
      // For elements that not FractionalType could be
      // left.dataType.asInstanceOf[ArrayType].containsNull || right.dataType.asInstanceOf[ArrayType].containsNull
    )
  }

  override def nullSafeEval(value1: Any, value2: Any): Any = {
    // Called from `eval` when both arguments are not null
    val array1 = value1.asInstanceOf[ArrayData]
    val array2 = value2.asInstanceOf[ArrayData]

    if (array1.numElements() != array2.numElements()) null
    else nullSafeEvalImplBase(array1, array2)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Null safe branch; if input is null: base class do the job

    val fun: (String, String) => String =
      (array1, array2) => {
        val expr = ctx.addReferenceObj(this.getClass.getName, this)
        // N.B. reference won't work if jar was added by `spark.sql(add jar ...)`
        s"""
        ${ev.value} = (ArrayData)$expr.nullSafeEval($array1, $array2);
        ${ev.isNull} = ${ev.value} == null;
       """
      }

    nullSafeCodeGen(ctx, ev, fun)
  }

  protected def nullSafeEvalImplBase(array1: ArrayData, array2: ArrayData): ArrayData = {
    // Called by `nullSafeEval`, arguments are not null here and size is checked already
    val res = new Array[Any](array1.numElements())

    var elem: Any = null
    var x1, x2: java.lang.Double = null

    // Could be optimized: use while loop, unboxed ops
    res.indices.foreach { i =>
      elem = null

      if (!array1.isNullAt(i) && !array2.isNullAt(i)) {

        x1 = toDouble(array1.get(i, elementType))
        if (!x1.isNaN && !x1.isInfinite) {
          x2 = toDouble(array2.get(i, elementType))
          if (!x2.isNaN && !x2.isInfinite)
            elem = toElementType(binaryOp(x1, x2))
        }

      }

      res.update(i, elem)
    }

    new GenericArrayData(res)
  }

  @transient protected lazy val toDouble: Any => jDouble = elementType match {
    case FloatType => x => x.asInstanceOf[Float].toDouble
    case DoubleType => x => x.asInstanceOf[jDouble]
    case IntegerType => x => x.asInstanceOf[Int].toDouble
    case LongType => x => x.asInstanceOf[Long].toDouble
    case ByteType => x => x.asInstanceOf[Byte].toDouble
    case ShortType => x => x.asInstanceOf[Short].toDouble
    case DecimalType() => x => x.asInstanceOf[Decimal].toDouble
    case _ => throw new AnalysisException(s"Unsupported element type: ${elementType.sql}")
  }

  @transient protected lazy val toElementType: jDouble => Any = elementType match {
    case FloatType => x => x.toFloat
    case DoubleType => x => x
    case IntegerType => x => x.toInt
    case LongType => x => x.toLong
    case ByteType => x => x.toByte
    case ShortType => x => x.toShort
    case DecimalType.Fixed(precision, scale) => x => Decimal(BigDecimal(x), precision, scale)
    case _ => throw new AnalysisException(s"Unsupported element type: ${elementType.sql}")
  }
}

object Types {
  type MWAJF = mutable.WrappedArray[jFloat]
  type AJF = Array[jFloat]
}
