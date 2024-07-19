/**
 * Created by vasnake@gmail.com on 2024-07-19
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.apache.spark.sql.catalyst.expressions.{
  Expression, ExpressionDescription
}

import org.apache.spark.sql.types._

import java.lang.{Float => jFloat, Double => jDouble}

import base.GenericUnaryPredicateNotNull

/**
  * Evaluates to `true` iff expression is not in (null, NaN, Infinity, -Infinity)
  */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns false if `expr` is in (null, NaN, Infinity, -Infinity), otherwise return true",
  examples = """
    Examples:
      > SELECT _FUNC_(cast('Infinity' as double));
       false
  """,
  since = "0.1.0")
case class GenericIsFinite(child: Expression) extends GenericUnaryPredicateNotNull {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))

  override def onNullInput: Any = false // not finite
  override def javaOnNullInput: String = "false"

  // Use unboxed ops; use lazy val for compute function
  override def nullSafeEval(input: Any): Any = {
    input match {
      case x: Double => !(jDouble.isNaN(x) || jDouble.isInfinite(x))
      case x: Float => !(jFloat.isNaN(x) || jFloat.isInfinite(x))
      case _ => true // Should never happen
    }
  }

  override def javaNullSafeEval(x: Any): String = s"!(Double.isNaN($x) || Double.isInfinite($x))"

}
