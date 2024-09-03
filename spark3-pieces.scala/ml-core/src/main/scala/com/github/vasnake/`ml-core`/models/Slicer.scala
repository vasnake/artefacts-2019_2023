/** Created by vasnake@gmail.com on 2024-07-12
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.core.num.VectorToolbox

// TODO: extend transformer trait
case class Slicer(columns: Array[Int]) {
  require(columns.nonEmpty, "columns indices must be provided")

  def transform(input: Array[Double]): Array[Double] = {
    require(input.nonEmpty, "input vector must not be empty")

    VectorToolbox.selectIndexed(input, columns)
  }
}
