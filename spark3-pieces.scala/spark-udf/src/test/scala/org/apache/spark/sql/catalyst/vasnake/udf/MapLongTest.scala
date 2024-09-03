/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark
import org.apache.spark.sql.DataFrame
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class MapLongTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {
  import Fixtures._

  private val transformExpr =
    """
    map_from_entries(
      arrays_zip(
          map_keys(feature_map),
          transform(map_values(feature_map), _x -> if(_x in (null, 'NaN', 'Infinity', '-Infinity'), null, _x))
      )
    )
  """

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", s"${transformExpr} as feature_map")
      .selectExpr("part", "uid", "cast(feature_map as map<string, long>) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "MapType(StringType,LongType,true)"

  it should "produce null item if all items are invalid, part G" in {
    val input = inputDF.where("part = 'G'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> null, 1 -> 7)")
    minAndCheck(input, "Map(2 -> null, 1 -> 3)")
    maxAndCheck(input, "Map(2 -> null, 1 -> 4)")
    avgAndCheck(input, s"Map(2 -> null, 1 -> ${7 / 2})")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> 5, 1 -> 7)")
    minAndCheck(input, "Map(2 -> 2, 1 -> 3)")
    maxAndCheck(input, "Map(2 -> 3, 1 -> 4)")
    avgAndCheck(input, s"Map(2 -> ${5 / 2}, 1 -> ${7 / 2})")
  }

  it should "process string keys, part K" in {
    val input = inputDF.where("part = 'K'")
    show(input, message = "input")
    sumAndCheck(input, "Map(foo -> 1, 1 -> 0)")
    minAndCheck(input, "Map(foo -> 0, 1 -> 0)")
    maxAndCheck(input, "Map(foo -> 1, 1 -> 0)")
    avgAndCheck(input, s"Map(foo -> ${1 / 4}, 1 -> ${0 / 4})")
  }

  it should "process double keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").double_long
    show(input, message = "input")
    sumAndCheck2(input, "Map(1.0 -> 7, 2.0 -> 5)", "MapType(DoubleType,LongType,true)")
    avgAndCheck2(
      input,
      s"Map(1.0 -> ${7 / 2}, 2.0 -> ${5 / 2})",
      "MapType(DoubleType,LongType,true)"
    )
  }

  it should "process float keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").float_long
    show(input, message = "input")
    sumAndCheck2(input, "Map(1.0 -> 7, 2.0 -> 5)", "MapType(FloatType,LongType,true)")
    avgAndCheck2(
      input,
      s"Map(1.0 -> ${7 / 2}, 2.0 -> ${5 / 2})",
      "MapType(FloatType,LongType,true)"
    )
  }
}
