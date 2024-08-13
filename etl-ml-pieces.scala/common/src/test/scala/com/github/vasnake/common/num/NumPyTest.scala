/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package com.github.vasnake.common.num

import org.scalatest._
import flatspec._
import matchers._
import org.scalactic.Equality

import com.github.vasnake.test.Conversions
import com.github.vasnake.test.Conversions.floatsAreEqual
import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
import com.github.vasnake.common.num.NumPy._
//import com.github.vasnake.core.num.NumPy._

class NumPyTest  extends AnyFlatSpec with should.Matchers {

  import Conversions.implicits.SeqOfDouble

  it should "produce reference percentile" in {
    val accuracy: Int = 5 // x.formatted(s"%1.${accuracy}f")
    implicit val seqFloatEquals: Equality[Seq[Float]] = createSeqFloatsEquality((a, b) => floatsAreEqual(a, b, (1d / math.pow(10, accuracy + 1)).toFloat))

    val xs = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).map(_.toDouble).toArray
    val pcs = Seq(0.0, 10.0, 30.0, 60.0, 100.0).toArray
    val expected = Seq(0.0, 0.9, 2.6999999999999997, 5.3999999999999995, 9.0)

    val ps = percentile(xs, pcs, sorted = true)

    assert(ps.toSeq.toString(accuracy) === expected.toString(accuracy))
    assert(ps.toSeq.toFloat === expected.toFloat)
  }

}
