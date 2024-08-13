/** Created by vasnake@gmail.com on 2024-07-16
  */
package com.github.vasnake.spark.udf.`java-api`

import java.lang.{ Float => jFloat, String => jString }

import scala.collection.mutable

import com.github.vasnake.core.num.VectorToolbox.isInvalid
import com.github.vasnake.core.num.implicits.checkNumFloat
import org.apache.spark.sql.api.java.UDF2

/** map_values_ordered(mapColumn, keysArray)
  *
  * pyspark: sparkSession.udf.registerJavaFunction("map_values_ordered", "com.github.vasnake.spark.udf.java-api.MapValuesOrderedUDF")
  */
class MapValuesOrderedUDF
    extends UDF2[Map[jString, jFloat], mutable.WrappedArray[jString], Array[jFloat]] {
  override def call(kvs: Map[jString, jFloat], keys: mutable.WrappedArray[jString])
    : Array[jFloat] = {
    val res = new Array[jFloat](keys.length)

    // Could be optimized using while loop and unboxed float ops
    keys
      .indices
      .foreach(i =>
        res.update(
          i,
          norm(kvs.getOrElse(keys(i), null)),
        )
      )

    res
  }

  private def norm(x: jFloat): jFloat = if (isInvalid(x)) null else x
}
