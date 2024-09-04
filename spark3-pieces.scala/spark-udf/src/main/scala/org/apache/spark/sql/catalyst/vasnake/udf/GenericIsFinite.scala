/** Created by vasnake@gmail.com on 2024-07-19
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import java.lang.{ Double => jDouble, Float => jFloat }

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.vasnake.udf.base.GenericUnaryPredicateNotNull
import org.apache.spark.sql.types._

/** Evaluates to `true` iff expression is not in (null, NaN, Infinity, -Infinity)
  */
@ExpressionDescription(
  usage =
    "_FUNC_(expr) - Returns false if `expr` is in (null, NaN, Infinity, -Infinity), otherwise return true",
  examples = """
    Examples:
      > SELECT _FUNC_(cast('Infinity' as double));
       false
  """,
  since = "0.1.0"
)
case class GenericIsFinite(child: Expression) extends GenericUnaryPredicateNotNull {
  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType))
  override def onNullInput: Any = false // not finite
  override def javaOnNullInput: String = "false"

  // Use unboxed ops; use lazy val for compute function
  override def nullSafeEval(input: Any): Any =
    input match {
      case x: Double => !(jDouble.isNaN(x) || jDouble.isInfinite(x))
      case x: Float => !(jFloat.isNaN(x) || jFloat.isInfinite(x))
      case _ => true // Should never happen
    }

  override def javaNullSafeEval(x: Any): String = s"!(Double.isNaN($x) || Double.isInfinite($x))"
}
