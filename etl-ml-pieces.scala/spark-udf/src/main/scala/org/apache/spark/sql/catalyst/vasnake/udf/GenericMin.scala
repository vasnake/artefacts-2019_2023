/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.common.num.FastMath
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.vasnake.udf.accum.{ NumericAccumulator => AccImpl }
import org.apache.spark.sql.catalyst.vasnake.udf.base.GenericAggregate

@ExpressionDescription(usage =
  """_FUNC_(expr) - Returns the min calculated from values of a group."""
)
case class GenericMin(
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0,
) extends GenericAggregate {
  def this(child: Expression) = this(child, 0, 0) // n.b. wanted by registry

  override def prettyName: String = "generic_min"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  @inline def combineItems(x: AccImpl.V, y: AccImpl.V): AccImpl.V =
    if (x == null) y
    else if (y == null) x
    else if (x.isNaN) y
    else if (y.isNaN) x
    else FastMath.min(x, y)
}
