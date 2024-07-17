/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.common.num

import org.apache.commons.math3.util.{ FastMath => fm }

object FastMath {
  val PI: Double = fm.PI
  val log: Double => Double = x => fm.log(x)
  val abs: Double => Double = x => fm.abs(x)
  val sqrt: Double => Double = x => fm.sqrt(x)
  val exp: Double => Double = x => fm.exp(x)
  val atan: Double => Double = x => fm.atan(x)
  val round: Double => Double = x => fm.round(x)

//  def min[@specialized(Double, Float, Int, Long) T](x: T, y: T): T = fm.min(x, y)
  val min: (Double, Double) => Double = (x, y) => fm.min(x, y)
}
