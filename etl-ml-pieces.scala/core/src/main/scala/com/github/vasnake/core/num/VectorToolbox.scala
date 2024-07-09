/**
 * Created by vasnake@gmail.com on 2024-07-09
 */
package com.github.vasnake.core.num

import scala.reflect.ClassTag

object VectorToolbox {

  def selectIndexed[
    @specialized(Double, Float) T: ClassTag
  ](src: Array[T], indices: Array[Int]): Array[T] = {

    if (src.isEmpty || indices.isEmpty) Array.empty
    else {
      // faster than indices.map ...
      val selected = new Array[T](indices.length)

      var trgIdx: Int = 0
      indices.foreach(srcIdx => {
        selected(trgIdx) = src(srcIdx)
        trgIdx += 1
      })

      selected
    }
  }

}
