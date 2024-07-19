/**
 * Created by vasnake@gmail.com on 2024-07-19
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.types._

import java.lang.{Float => jFloat, Double => jDouble}

import base.GenericUnaryPredicateBoolNotNull

/**
  * Evaluates to `true` iff it's Infinity (or -Infinity).
  */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if `expr` is in (Infinity, -Infinity), otherwise return false",
  examples = """
    Examples:
      > SELECT _FUNC_(cast('Infinity' as double));
       true
  """,
  since = "0.1.0")
case class GenericIsInf(child: Expression) extends GenericUnaryPredicateBoolNotNull {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))

  override def onNullInput: Any = false // not infinity
  override def javaOnNullInput: String = "false"

  // Use unboxed ops; use lazy val for compute function
  override def nullSafeEval(input: Any): Any = {
    input match {
      case x: Double => jDouble.isInfinite(x)
      case x: Float => jFloat.isInfinite(x)
      case _ => false // Should never happen
    }
  }

  override def javaNullSafeEval(x: Any): String = s"Double.isInfinite($x)"

}
