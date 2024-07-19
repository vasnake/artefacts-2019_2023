/**
 * Created by vasnake@gmail.com on 2024-07-19
 */
package org.apache.spark.sql.catalyst.vasnake.udf.base

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, CodeGenerator, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.{
  UnaryExpression, ImplicitCastInputTypes, Predicate
}

// TODO: rename to GenericUnaryPredicateNotNull
trait GenericUnaryPredicateBoolNotNull extends UnaryExpression with Predicate with ImplicitCastInputTypes {
  // Base implementation

  override final def nullable: Boolean = false

  override def eval(input: InternalRow): Any = nullUnsafeEval(child.eval(input))

  private def nullUnsafeEval(input: Any): Any = {
    if (input == null) onNullInput
    else nullSafeEval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)

    // Avoid using boxing/unboxing
    ev.copy(
      code = code"""
        ${eval.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        ${ev.value} = ${eval.isNull} ? (boolean) $javaOnNullInput : (boolean) ${javaNullSafeEval(eval.value)};
      """,
      isNull = FalseLiteral
    )
  }

  // Custom logic

  // w/o codegen
  def onNullInput: Any
  override def nullSafeEval(input: Any): Any = ???

  // with codegen
  def javaOnNullInput: String
  def javaNullSafeEval(input: Any): String

}
