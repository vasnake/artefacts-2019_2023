/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.apache.spark.sql._

object ArrayFloatFixture {
  type AFC = Option[Array[Option[Float]]] // Array of Float Column

  val NAN: Float = Float.NaN
  val PINF: Float = Float.PositiveInfinity
  val NINF: Float = Float.NegativeInfinity

  def df(
    rows: List[(String, AFC, AFC, AFC)]
  )(implicit
    spark: SparkSession
  ): DataFrame = {
    import spark.implicits._
    rows.toDF("uid", "va", "vb", "expected")
  }

  def af(xs: Any*): AFC =
    Some(
      xs.map {
        case None => None
        case x: Int => Some(x.toFloat)
        case x: Double => Some(x.toFloat)
        case x => Some(x.asInstanceOf[Number].floatValue())
      }.toArray[Option[Float]]
    )
}
