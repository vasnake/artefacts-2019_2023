/**
 * Created by vasnake@gmail.com on 2024-07-08
 */
package com.github.vasnake.text.parser

import scala.util.parsing.combinator.RegexParsers

object JoinExpressionParser {

  sealed abstract class Expression
  case class Node(name: String) extends Expression
  case class Tree(left: Expression, right: Expression, joinOp: String) extends Expression

  def apply(rule: String): Expression = {
    val p = new Parser
    p.parseAll(p.expr, rule) match {
      case p.Success(result: Expression, _) => result
      case e @ p.NoSuccess(_, _) => throw new IllegalArgumentException(s"JoinRuleParser has failed: `${e.toString}`")
    }
  }

  class Parser extends RegexParsers {
    // expr = operand (operator operand)+
    // operand = name | (expr)

    def expr: Parser[Expression] = operand ~ rep(operator ~ operand) ^^ {
      case left ~ lst => makeTree(left, lst)
    }

    def operand: Parser[Expression] = name ^^ { n => Node(n) } | "(" ~> expr <~ ")"
    def operator: Parser[String] = """\w+""".r
    def name: Parser[String] = """\w+""".r

    @scala.annotation.tailrec
    private def makeTree
    (
      left: Expression,
      rest: List[String ~ Expression]
    ): Expression =
      rest match {
        case Nil => left
        case h :: t => makeTree(Tree(left, h._2, h._1), t)
      }

  }

}
