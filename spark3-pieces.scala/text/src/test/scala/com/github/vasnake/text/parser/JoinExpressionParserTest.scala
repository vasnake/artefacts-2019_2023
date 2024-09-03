/**
 * Created by vasnake@gmail.com on 2024-08-16
 */
package com.github.vasnake.text.parser

import org.scalatest.flatspec._
import org.scalatest.matchers._

class JoinExpressionParserTest extends AnyFlatSpec with should.Matchers {

  it should "parse join rules" in {
    assert(JoinExpressionParser("a inner b").toString === "Tree(Node(a),Node(b),inner)")
    assert(JoinExpressionParser("groups_42 left_outer profs_source").toString === "Tree(Node(groups_42),Node(profs_source),left_outer)")
    assert(JoinExpressionParser("a inner (b outer c)").toString === "Tree(Node(a),Tree(Node(b),Node(c),outer),inner)")

    assert(JoinExpressionParser("(a inner b) outer c").toString === "Tree(Tree(Node(a),Node(b),inner),Node(c),outer)")
    assert(JoinExpressionParser("a inner b outer c").toString   === "Tree(Tree(Node(a),Node(b),inner),Node(c),outer)")

    assert(JoinExpressionParser("a inner b outer c cross d").toString       === "Tree(Tree(Tree(Node(a),Node(b),inner),Node(c),outer),Node(d),cross)")
    assert(JoinExpressionParser("(((a inner b) outer c) cross d)").toString === "Tree(Tree(Tree(Node(a),Node(b),inner),Node(c),outer),Node(d),cross)")

    assert(JoinExpressionParser("(a inner b) outer (c cross d)").toString === "Tree(Tree(Node(a),Node(b),inner),Tree(Node(c),Node(d),cross),outer)")
    assert(JoinExpressionParser("a inner (b outer c) cross d").toString === "Tree(Tree(Node(a),Tree(Node(b),Node(c),outer),inner),Node(d),cross)")
    assert(JoinExpressionParser("(a inner (b outer (c cross d)))").toString === "Tree(Node(a),Tree(Node(b),Tree(Node(c),Node(d),cross),outer),inner)")
  }
}
