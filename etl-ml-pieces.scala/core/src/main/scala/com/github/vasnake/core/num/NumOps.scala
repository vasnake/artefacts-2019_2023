/** Created by vasnake@gmail.com on 2024-07-16
  */
package com.github.vasnake.core.num

import java.lang.{ Double => jDouble, Float => jFloat }

trait NumOpsI[A] {
  def isNaN(x: A): Boolean
  def isInfinite(x: A): Boolean
}

object implicits {
  implicit val checkNumDouble: NumOpsI[jDouble] = new NumOpsI[jDouble] {
    override def isNaN(x: jDouble): Boolean = x.isNaN // TODO: compare performance compared with: java.lang.Double.isNaN(x.doubleValue())
    override def isInfinite(x: jDouble): Boolean = x.isInfinite
  }

  implicit val checkNumFloat: NumOpsI[jFloat] = new NumOpsI[jFloat] {
    override def isNaN(x: jFloat): Boolean = x.isNaN
    override def isInfinite(x: jFloat): Boolean = x.isInfinite
  }
}
