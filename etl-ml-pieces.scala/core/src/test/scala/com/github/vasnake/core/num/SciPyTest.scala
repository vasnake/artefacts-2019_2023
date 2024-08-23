/** Created by vasnake@gmail.com on 2024-08-13
  */
package com.github.vasnake.core.num

import com.github.vasnake.core.num.SciPy._
import com.github.vasnake.test.Conversions.floatsAreEqual
import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class SciPyTest extends AnyFlatSpec with should.Matchers {
  it should "find reference pchip interpolator coefficients" in {
    val xs = Array(0.0, 0.001, 0.016455, 0.4757729, 0.6251235, 0.999, 0.99900001, 1.0)
    val ys = Array(0.0, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.0)

    val actual = PCHIP.coefficients(xs, ys)

    val expected = Array(
      Array(0.0, 0.0, 0.2, 0.4, 0.6, 0.8, 1.0),
      Array(0.0, 0.0, 0.38202461, 0.4358122, 0.70150493, 0.8, 1.0),
      Array(0.0, 0.19387529, 0.28986194, 0.55945233, 0.6, 1.0, 1.0),
      Array(0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.0)
    )

    def _check(a: Array[Double], e: Array[Double]) = {
      implicit val seqFloatEquals =
        createSeqFloatsEquality((a, b) => floatsAreEqual(a, b, 0.00000001f))
      assert(a.toSeq.map(_.toFloat) === e.toSeq.map(_.toFloat))
    }

    expected.indices.foreach(rowIdx => _check(actual(rowIdx), expected(rowIdx)))
  }
}
