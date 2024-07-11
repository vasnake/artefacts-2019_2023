/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.common.num

import org.apache.commons.math3.util.{ FastMath => fm }

object FastMath {
  val log: Double => Double = x => fm.log(x)
  val abs: Double => Double = x => fm.abs(x)
  val sqrt: Double => Double = x => fm.sqrt(x)
}
