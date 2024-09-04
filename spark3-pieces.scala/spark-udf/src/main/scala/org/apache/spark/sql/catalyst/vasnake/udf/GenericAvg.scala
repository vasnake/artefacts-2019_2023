/** Created by vasnake@gmail.com on 2024-07-17
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.vasnake.udf.accum.{ NumericAccumulatorWithCount => AccImpl, _ }
import org.apache.spark.sql.catalyst.vasnake.udf.base.GenericAggregateNumWithCount

@ExpressionDescription(usage =
  """_FUNC_(expr) - Returns the avg calculated from values of a group."""
)
case class GenericAvg(
  child: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0
) extends GenericAggregateNumWithCount {
  def this(child: Expression) = this(child, 0, 0) // n.b. wanted by registry

  override def prettyName: String = "generic_avg"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)

  @inline def combineItems(x: AccImpl.V, y: AccImpl.V): AccImpl.V = {
    debug(s"combineItems enter, x: `${x}`, y: `${y}`")

    if (x == null) y
    else if (y == null) x
    else if (x.value == null) y
    else if (y.value == null) x
    else if (x.value.isNaN) y
    else if (y.value.isNaN) x
    else
      AccumulatorValue(
        count = x.count + y.count,
        value = x.value + y.value
      )
  }

  @inline def evalItem(x: AccImpl.V): AccImpl.VN =
    if (x == null || x.value == null) null.asInstanceOf[AccImpl.VN]
    else x.value / x.count
}
