/** Created by vasnake@gmail.com on 2024-07-09
  */
package com.github.vasnake.core.num

import scala.collection.mutable

import com.github.vasnake.core.num.NumPy._

object SciPy {

  /** PCHIP: Piecewise Cubic Hermite Interpolating Polynomial;
    * CDF: Cumulative Distribution Function.
    *
    * Implementation of scipy.interpolate PCHIP fitting methods
    */
  object PCHIP {
    // TODO: optimize, add docs, tests

    // https://stackoverflow.com/questions/43938418/scala-division-by-zero-yields-different-results/43949981
    //  0 / 0 is an integer division by zero (as both arguments are integer literals), and that is required to throw a java.lang.ArithmeticException.
    //  1.toDouble/0.toDouble is a floating point division by zero with a positive numerator, and that is required to evaluate to +Infinity.
    //  0.0/0.0 is a floating point division by zero with a zero numerator, and that is required to evaluate to +NaN.
    //  The first is a Java and Scala convention, the other two are properties of IEEE754 floating point, which is what Java and Scala both use.
    def coefficients(xs: Array[Double], ys: Array[Double]): Array[Array[Double]] = {
      val dk: Array[Double] = findDerivatives(xs, ys)
      fromDerivatives(xs, ys, dk)
    }

    def findDerivatives(xs: Array[Double], ys: Array[Double]): Array[Double] = {

      val hk = xs.indices.foldLeft(mutable.ArrayBuffer.empty[Double]) { (acc, idx) =>
        if (idx > 0) acc.append(xs(idx) - xs(idx - 1))

        acc
      }

      val mk = ys.indices.foldLeft(mutable.ArrayBuffer.empty[Double]) { (acc, idx) =>
        if (idx > 0) acc.append((ys(idx) - ys(idx - 1)) / hk(idx - 1))

        acc
      }

      if (ys.length == 2) Array(mk(0), mk(0))
      else {
        val smk = mk.map(_.signum)
        val w1 = (1 until hk.length).map(idx => 2.0 * hk(idx) + hk(idx - 1))
        val w2 = (1 until hk.length).map(idx => hk(idx) + 2.0 * hk(idx - 1))

        val condition = (1 until smk.length).map { idx =>
          smk(idx) != smk(idx - 1) || mk(idx) == 0 || mk(idx - 1) == 0
        }

        val wmean = (1 until mk.length).map { idx =>
          (w1(idx - 1) / mk(idx - 1) + w2(idx - 1) / mk(idx)) / (w1(idx - 1) + w2(idx - 1))
        }

        val dk = new Array[Double](ys.length)
        (1 until dk.length - 1).foreach(idx =>
          if (!condition(idx - 1)) dk(idx) = 1.0 / wmean(idx - 1)
        )

        dk(0) = PCHIP._edgeCase(hk.head, hk(1), mk.head, mk(1))
        dk(dk.length - 1) = PCHIP._edgeCase(hk.last, hk(hk.length - 2), mk.last, mk(hk.length - 2))

        dk
      }

    }

    def fromDerivatives(
      xs: Array[Double],
      ys: Array[Double],
      dk: Array[Double],
    ): Array[Array[Double]] = {
      require(
        xs.length == ys.length && ys.length == dk.length,
        "xi and yi and dk need to have the same length",
      )
      require(
        xs.indices.forall(idx => idx == 0 || (xs(idx) - xs(idx - 1)) > 0),
        "x coordinates are not in increasing order",
      )

      val m = xs.length - 1
      // order k: always 4, global order: 3, cubic interpolator
      val c = new Array[Array[Double]](m)

      (0 until m).foreach { idx =>
        val b: Array[Double] = _constructFromDerivatives(
          xs(idx),
          xs(idx + 1),
          ys(idx),
          ys(idx + 1),
          dk(idx),
          dk(idx + 1),
        )
        c(idx) = b
      }

      transpose(c)
    }

    def _constructFromDerivatives(
      xa: Double,
      xb: Double,
      ya: Double,
      yb: Double,
      da: Double,
      db: Double,
    ): Array[Double] = {
      val c = new Array[Double](4)

      c(0) = ya / 1.0 * 1.0
      c(1) = da / 3.0 * (xb - xa)
      c(1) -= -1.0 * 1.0 * c.head

      c(3) = yb / 1.0 * 1.0 * 1.0
      c(2) = db / 3.0 * -1.0 * (xb - xa)
      c(2) -= -1 * 1.0 * c(3)

      c
    }

    def _edgeCase(
      h0: Double,
      h1: Double,
      m0: Double,
      m1: Double,
    ): Double = {
      val d = ((2.0 * h0 + h1) * m0 - h0 * m1) / (h0 + h1)

      val mask = d.signum != m0.signum
      val mask2 = (m0.signum != m1.signum) && (d.abs > 3.0 * m0.abs)
      val mmm = !mask && mask2

      if (mmm) 3.0 * m0
      else if (mask) 0.0
      else d
    }
  }

  /** simplified copy-paste from scipy/interpolate/_ppoly.so evaluate_bernstein
    *
    * https://github.com/scipy/scipy/blob/3cfd10e463da4c04732639ceafd9427bf3bb2a8a/scipy/interpolate/_ppoly.pyx#L1126
    */
  object PPolyBernsteinCubic {
    def interpolate(
      xval: Double,
      coefficients: Array[Array[Double]],
      intervals: Array[Double],
    ): Double = {
      val idx = searchSorted(intervals, xval)

      if (idx < 0) Double.NaN
      else
        evaluateBpoly(
          (xval - intervals(idx)) / (intervals(idx + 1) - intervals(idx)),
          coefficients,
          idx,
        )
    }

    @inline private def evaluateBpoly(
      point: Double,
      coefficients: Array[Array[Double]],
      idx: Int,
    ): Double = {
      val one = 1.0 - point

      coefficients(0)(idx) * one * one * one +
        coefficients(1)(idx) * 3.0 * one * one * point +
        coefficients(2)(idx) * 3.0 * one * point * point +
        coefficients(3)(idx) * point * point * point
    }
  }
}
