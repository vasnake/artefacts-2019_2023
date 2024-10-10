/** Created by vasnake@gmail.com on 2024-07-09
  */
package com.github.vasnake.common.num

import org.apache.commons.math3.stat.descriptive.rank.Percentile

object NumPy {

  /** Find `xs` percentiles for each percent in `ps`, see `numpy.lib.function_base.percentile`.
    *
    * @param xs     values list to search in, should be sorted
    * @param ps     list of percents, values domain: (0, 100]; if percent == 0 it will be passed to lib as 0.0000001
    * @param sorted indicates state of the `xs` data; `xs` will be sorted if `sorted` == false
    * @return percentile for each `ps` element
    */
  def percentile(
    xs: Array[Double],
    ps: Array[Double],
    sorted: Boolean = false
  ): Array[Double] = {
    // it could be faster, see https://github.com/scalanlp/breeze/blob/master/math/src/main/scala/breeze/stats/DescriptiveStats.scala

    val pc = new Percentile()
      .withEstimationType(Percentile.EstimationType.R_7)

    pc.setData(if (sorted) xs else xs.sorted)

    ps.map(p =>
      if (p <= 0d) pc.evaluate(0.0000001)
      else pc.evaluate(p)
    )
  }
}
