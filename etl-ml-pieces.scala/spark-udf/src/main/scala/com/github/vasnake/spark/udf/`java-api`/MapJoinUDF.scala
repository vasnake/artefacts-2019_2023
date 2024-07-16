/**
 * Created by vasnake@gmail.com on 2024-07-16
 */
package com.github.vasnake.spark.udf.`java-api`

import org.apache.spark.sql.api.java.UDF3
import java.lang.{Float => jFloat}

import com.github.vasnake.core.num.VectorToolbox.isInvalid
import com.github.vasnake.core.num.implicits.checkNumFloat

/**
  * map_join(mapColumn, itemsSeparator, kvSeparator), e.g. `map_join(score_map, ',', ':') as csv`
  *
  * inspired by `spark.sql.functions.array_join`
  *
  * sparkSession.udf.registerJavaFunction("map_join", "com.github.vasnake.spark.udf.`java-api`.MapJoinUDF")
  */
class MapJoinUDF extends UDF3[Map[String, jFloat], String, String, String] {
  override def call(feature: Map[String, jFloat], itemsSep: String, kvSep: String): String = {
    feature.map { case (k, v) => s"$k$kvSep${norm(v)}" }
      .mkString(itemsSep)
  }

  private def norm(x: jFloat): jFloat = if (isInvalid(x)) null else x
}
