/**
 * Created by vasnake@gmail.com on 2024-07-17
 */
package org.apache.spark.sql.catalyst.vasnake.udf

// References:
// `case class UserDefinedFunction protected[sql] (` at sql/core/src/main/scala/org/apache/spark/sql/expressions/UserDefinedFunction.scala
// `case class ScalaUDF(` at sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ScalaUDF.scala
// `case class ArrayIntersect(` at sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala

// UserDefinedFunction and ScalaUDF won't work in generic form: you can't detect, preserve and maintain actual data types.

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

import java.lang.{Double => jDouble}

import org.apache.spark.sql.catalyst.vasnake.udf.base.GenericBinaryArraysElements

/**
  * Returns an array of the elements produced by multiplication of elements with same index from input arrays.
  * Registration: `functions.registerAs("generic_coomul", "coomul", spark, overrideIfExists = true)`
  */
@ExpressionDescription(
  usage = """
  _FUNC_(array1, array2) - Returns an array of the elements where result[i] = array1[i] * array2[i].
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(4, 5, 6));
       [4, 10, 18]
  """,
  since = "0.1.0")
case class GenericVectorCooMul(left: Expression, right: Expression) extends GenericBinaryArraysElements {
  def binaryOp(x1: jDouble, x2: jDouble): jDouble = x1 * x2
}
