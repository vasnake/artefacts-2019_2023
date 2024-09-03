/** Created by vasnake@gmail.com on 2024-08-13
  */
package com.github.vasnake.core.num

import com.github.vasnake.core.num.NumPy._
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class NumPyTest extends AnyFlatSpec with should.Matchers {
//  import Conversions.implicits.SeqOfDouble
  it should "make array slices like python" in {
    def check(
      xs: Array[Int],
      expected: Array[Int],
      start: Int,
      stop: Int,
      step: Int
    ) = assert(
      slice(xs.map(_.toDouble), start, stop, step).map(_.toInt).mkString(",") === expected.mkString(
        ","
      )
    )

    val xs: Array[Int] = Array(1, 2, 3, 4, 5)

    check(xs, Array(1, 2, 3, 4, 5), 0, 5, 1)
    check(xs, Array(1, 3, 5), 0, 5, 2)
    check(xs, Array(1, 4), 0, 5, 3)
    check(xs, Array(1, 5), 0, 5, 4)
    check(xs, Array(1), 0, 5, 5)
    check(xs, Array(1), 0, 5, 6)

    check(xs, Array(1, 2, 3, 4), 0, 4, 1)
    check(xs, Array(1, 3), 0, 4, 2)
    check(xs, Array(1, 4), 0, 4, 3)
    check(xs, Array(1), 0, 4, 4)
    check(xs, Array(1), 0, 4, 5)

    check(xs, Array(2, 3, 4), 1, 4, 1)
    check(xs, Array(2, 4), 1, 4, 2)
    check(xs, Array(2), 1, 4, 3)
    check(xs, Array(2), 1, 4, 4)

  }

  it should "produce reference histogram" in {
    val xs: Array[Double] = Array(0.001, 0.016455, 0.4757729, 0.6251235, 0.999)
    val bins: Array[Double] =
      Array(0.0, 0.001, 0.016455, 0.4757729, 0.6251235, 0.999, 0.999000001, 1.0)

    val hist = histogram(xs, bins)
    assert(hist.toSeq === Seq(0, 1, 1, 1, 1, 1, 0))
  }

  it should "use half-open intervals in histogram" in {
    val xs: Array[Double] = Array(1, 2, 1)
    val bins: Array[Double] = Array(0, 1, 2, 3)

    val hist = histogram(xs, bins)
    assert(hist.toSeq === Seq(0, 2, 1))
  }

  it should "produce reference cumsum" in {
    val xs = Array(0, 1, 1, 1, 1, 1, 0)
    assert(cumulativeSum(xs) === Array(0, 1, 2, 3, 4, 5, 5))
  }

  it should "digitize to reference bins" in {
    val bins = Seq(-1, 0.5, 0.9, 1.1, 2.7, 5.1, 5.4, 5.5).map(x =>
      digitize(x, bins = Array(Double.MinValue, 0.9, 2.7, 5.4), right = false)
    )
    assert(bins === Seq(1, 1, 2, 2, 3, 3, 4, 4))

    val bins2 =
      Seq(-1, 1, 1.5, 2, 2.5, 3, 3.5).map(x => digitize(x, bins = Array(1, 2, 3), right = false))
    assert(bins2 === Seq(0, 1, 1, 2, 2, 3, 3))

  }
}
