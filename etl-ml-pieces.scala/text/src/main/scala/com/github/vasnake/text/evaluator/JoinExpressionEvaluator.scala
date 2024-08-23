/** Created by vasnake@gmail.com on 2024-07-08
  */
package com.github.vasnake.text.evaluator

import com.github.vasnake.text.parser.JoinExpressionParser._

sealed trait JoinExpressionEvaluator[T] {
  def eval[B](
    op: (B, B, String) => B
  )(implicit
    conv: String => T,
    ev: T => B
  ): B
}

final case class SingleItemJoin[T](item: T) extends JoinExpressionEvaluator[T] {
  override def eval[B](
    op: (B, B, String) => B
  )(implicit
    conv: String => T,
    ev: T => B
  ): B =
    ev(item)
}

final case class TreeJoin[T](tree: Tree) extends JoinExpressionEvaluator[T] {
  override def eval[B](
    joinOp: (B, B, String) => B
  )(implicit
    dir: String => T,
    ev: T => B
  ): B =
    tree match {
      case Tree(left: Node, right: Node, jo) => joinOp(dir(left.name), dir(right.name), jo)
      case Tree(left: Node, right: Tree, jo) =>
        joinOp(dir(left.name), TreeJoin[T](right).eval(joinOp)(dir, ev), jo)
      case Tree(left: Tree, right: Node, jo) =>
        joinOp(TreeJoin[T](left).eval(joinOp)(dir, ev), dir(right.name), jo)
      case Tree(left: Tree, right: Tree, jo) =>
        joinOp(
          TreeJoin[T](left).eval(joinOp)(dir, ev),
          TreeJoin[T](right).eval(joinOp)(dir, ev),
          jo
        )
    }
}
