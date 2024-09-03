/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import java.lang.{ Double => jDouble }

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.vasnake.udf.base.GenericBinaryArraysElements

/** Returns an array of elements produced by `sum/2` of elements with same index from input arrays.
  * Registration: `functions.registerAs("generic_semisum", "semisum", spark, overrideIfExists = true)`
  */
@ExpressionDescription(
  usage = """
  _FUNC_(array1, array2) - Returns an array of elements where result[i] = (array1[i] + array2[i]) / 2.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1, 2, 3), array(4, 5, 6));
       [2.5, 3.5, 4.5]
  """,
  since = "0.1.0"
)
case class GenericVectorSemiSum(left: Expression, right: Expression)
    extends GenericBinaryArraysElements {
  def binaryOp(x1: jDouble, x2: jDouble): jDouble = (x1 + x2) / 2.0
}
