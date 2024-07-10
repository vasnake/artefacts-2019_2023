/** Created by vasnake@gmail.com on 2024-07-09
  */
package com.github.vasnake.core.num

import scala.collection.mutable
import scala.reflect.ClassTag

object NumPy {

  /** Return the index of the bin to which x value belongs. See `np.digitize`
    */
  def digitize(
    x: Double,
    bins: Array[Double],
    right: Boolean = false,
  ): Int = {
    // TODO: add tests
    /*
    def digitize(x, bins, right=False):
        """
        Return the indices of the bins to which each value in input array belongs.

        =========  =============  ============================
        `right`    order of bins  returned index `i` satisfies
        =========  =============  ============================
        ``False``  increasing     ``bins[i-1] <= x < bins[i]``
        ``True``   increasing     ``bins[i-1] < x <= bins[i]``
        ``False``  decreasing     ``bins[i-1] > x >= bins[i]``
        ``True``   decreasing     ``bins[i-1] >= x > bins[i]``
        =========  =============  ============================

        If values in `x` are beyond the bounds of `bins`, 0 or ``len(bins)`` is
        returned as appropriate.
     */

    val increasing = bins.head <= bins.last
    val idx = searchSorted(bins, x)

    if (right)
      ???
    else if (increasing)
      if (idx < 0 && x < bins.head) 0
      else if (idx < 0 && x > bins.last) bins.length
      else if (idx == bins.length - 2 && x >= bins.last) bins.length
      else idx + 1
    else
      ???

  }

  /** Return the index of the interval to which xval belongs.
    * If xval is out of bounds, return `-1`.
    *
    * Intervals must be sorted.
    */
  @inline def searchSorted(intervals: Array[Double], xval: Double): Int =
    // TODO: add tests and assertions

    if (intervals.last >= intervals.head)
      findIntervalAscending(intervals, xval)
    else
      findIntervalDescending(intervals, xval)

  @inline private def findIntervalAscending(intervals: Array[Double], xval: Double): Int = {
    // simplified copy-paste from
    // scipy/interpolate/_ppoly.so evaluate_bernstein
    // https://github.com/scipy/scipy/blob/3cfd10e463da4c04732639ceafd9427bf3bb2a8a/scipy/interpolate/_ppoly.pyx#L1126

    val len = intervals.length
    val last = intervals.last

    if (!(xval >= intervals.head && xval <= last))
      // out-of-bounds or nan
      -1
    else if (xval == last) len - 2
    else {
      // binary search
      var low: Int = 0
      var high: Int = len - 2
      var mid: Int = 0

      if (xval < intervals(low + 1)) high = low

      while (low < high) {
        mid = (high + low) / 2
        if (xval < intervals(mid)) high = mid
        else if (xval >= intervals(mid + 1)) low = mid + 1
        else {
          low = mid; high = 0
        } // found, break
      }

      low
    }
  }

  @inline private def findIntervalDescending(intervals: Array[Double], xval: Double): Int = {
    val len = intervals.length
    val last = intervals.last

    if (!(xval <= intervals.head && xval >= last))
      // out-of-bounds or nan
      -1
    else if (xval == last) len - 2
    else {
      // binary search
      var low = 0
      var high = len - 2
      var mid = 0

      if (xval > intervals(low + 1)) high = low

      while (low < high) {
        mid = (high + low) / 2
        if (xval > intervals(mid)) high = mid
        else if (xval <= intervals(mid + 1)) low = mid + 1
        else {
          low = mid; high = 0
        } // found, break
      }

      low
    }
  }

  /** quote from numpy docstring:
    * {{{
    *     All but the last (righthand-most) bin is half-open.  In other words,
    *     if `bins` is::
    *
    *    [1, 2, 3, 4]
    *
    *    then the first bin is ``[1, 2)`` (including 1, but excluding 2) and
    *     the second ``[2, 3)``.  The last bin, however, is ``[3, 4]``, which
    *    includes* 4.
    * }}}
    *
    * @param xs values to count
    * @param bins sorted (ordered low -> high) bins edges
    * @return histogram for xs on bins.length - 1 intervals
    */
  def histogram(xs: Array[Double], bins: Array[Double]): Array[Int] = {
    // TODO: add docs, tests; optimizations
    val len = bins.length
    require(len > 1, "Bins length must be not less than 2")

    def findBin(xval: Double): Int =
      if (xval <= bins.head) 0
      else if (xval >= bins.last) len - 2
      else {
        // binary search
        var low: Int = 0
        var high: Int = len - 2
        var mid: Int = 0

        if (xval < bins(low + 1)) high = low

        while (low < high) {
          mid = (high + low) / 2
          if (xval < bins(mid)) high = mid
          else if (xval >= bins(mid + 1)) low = mid + 1
          else { low = mid; high = 0 } // found, break
        }

        low
      }

    val hist = new Array[Int](bins.length - 1)

    xs.foreach { x =>
      val bin: Int = findBin(x)
      hist(bin) = hist(bin) + 1
    }

    hist
  }

  def transpose(arr2d: Array[Array[Double]]): Array[Array[Double]] = {
    // TODO: add shapes check, docs, tests

    val oldCols = arr2d.head.length
    val res = (0 until oldCols).map(_ => new Array[Double](arr2d.length)).toArray

    arr2d
      .indices
      .foreach(newCol =>
        (0 until oldCols).foreach(newRow => res(newRow)(newCol) = arr2d(newCol)(newRow))
      )

    res
  }

  def cumulativeSum[@specialized(Double, Float, Int) T: ClassTag](
    xs: Array[T]
  )(implicit
    ev: Numeric[T]
  ): Array[T] = {
    // TODO: add docs, tests, optimizations
    val buf = xs.foldLeft(mutable.ArrayBuffer.empty[T]) { (acc, itm) =>
      if (acc.nonEmpty) acc.append(ev.plus(itm, acc.last))
      else acc.append(itm)

      acc
    }

    buf.toArray
  }

  def slice(
    xs: Array[Double],
    start: Int,
    stop: Int,
    step: Int,
  ): Array[Double] = {
    // TODO: add negative indices; tests, docs, optimizations
    // more options here https://github.com/botkop/numsca

    val res = mutable.ArrayBuffer.empty[Double]

    if (start < xs.length && start >= 0 && stop > start) {
      var idx: Int = start
      val _stop = math.min(xs.length, stop)

      while (idx < _stop) {
        res.append(xs(idx))
        idx += step
      }
    }

    res.toArray
  }
}
