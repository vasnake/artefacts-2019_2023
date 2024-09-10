/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark
import org.apache.spark.sql.DataFrame
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class MapDecimalTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {
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
      .selectExpr("part", "uid", "cast(feature_map as map<string, decimal(4,3)>) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "MapType(StringType,DecimalType(4,3),true)"

  it should "produce null item if all items are invalid, part G" in {
    val input = inputDF.where("part = 'G'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> null, 1 -> 7.000)")
    minAndCheck(input, "Map(2 -> null, 1 -> 3.000)")
    maxAndCheck(input, "Map(2 -> null, 1 -> 4.000)")
    avgAndCheck(input, "Map(2 -> null, 1 -> 3.500)")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> 5.000, 1 -> 7.000)")
    minAndCheck(input, "Map(2 -> 2.000, 1 -> 3.000)")
    maxAndCheck(input, "Map(2 -> 3.000, 1 -> 4.000)")
    avgAndCheck(input, "Map(2 -> 2.500, 1 -> 3.500)")
  }

  it should "process string keys, part K" in {
    val input = inputDF.where("part = 'K'")
    show(input, message = "input")
    sumAndCheck(input, "Map(foo -> 2.000, 1 -> 1.000)")
    minAndCheck(input, "Map(foo -> 0.300, 1 -> 0.500)")
    maxAndCheck(input, "Map(foo -> 1.700, 1 -> 0.500)")
    avgAndCheck(input, "Map(foo -> 1.000, 1 -> 0.500)")
  }

  it should "process date keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").date_decimal
    show(input, message = "input")

    sumAndCheck2(input, "Map(2021-05-12 -> 5.000)", "MapType(DateType,DecimalType(4,3),true)")
    avgAndCheck2(input, "Map(2021-05-12 -> 2.500)", "MapType(DateType,DecimalType(4,3),true)")
  }

  it should "process time keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").time_decimal
    show(input, message = "input")

    sumAndCheck2(
      input,
      "Map(2021-05-12 12:34:55.0 -> 5.000)",
      "MapType(TimestampType,DecimalType(4,3),true)"
    )
    avgAndCheck2(
      input,
      "Map(2021-05-12 12:34:55.0 -> 2.500)",
      "MapType(TimestampType,DecimalType(4,3),true)"
    )
  }
}
