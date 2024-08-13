/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql.DataFrame

import com.github.vasnake.spark.test.LocalSpark

class MapIntTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

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
      .selectExpr("part", "uid", "cast(feature_map as map<string, integer>) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "MapType(StringType,IntegerType,true)"

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

  it should "process int keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").int_int
    val expectedType = "MapType(IntegerType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5, 1 -> 7)", expectedType)
    avgAndCheck2(input, s"Map(2 -> ${5 / 2}, 1 -> ${7 / 2})", expectedType)
  }

  it should "process long keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").long_int
    val expectedType = "MapType(LongType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5, 1 -> 7)", expectedType)
    avgAndCheck2(input, s"Map(2 -> ${5 / 2}, 1 -> ${7 / 2})", expectedType)
  }

  it should "process double keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").double_int
    val expectedType = "MapType(DoubleType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(1.0 -> 7, 2.0 -> 5)", expectedType)
    avgAndCheck2(input, s"Map(1.0 -> ${7 / 2}, 2.0 -> ${5 / 2})", expectedType)
  }

    it should "process float keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").float_int
    val expectedType = "MapType(FloatType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(1.0 -> 7, 2.0 -> 5)", expectedType)
    avgAndCheck2(input, s"Map(1.0 -> ${7 / 2}, 2.0 -> ${5 / 2})", expectedType)
  }

  it should "process byte keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").byte_int
    val expectedType = "MapType(ByteType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5, 1 -> 7)", expectedType)
    avgAndCheck2(input, s"Map(2 -> ${5 / 2}, 1 -> ${7 / 2})", expectedType)
  }

  it should "process short keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").short_int
    val expectedType = "MapType(ShortType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5, 1 -> 7)", expectedType)
    avgAndCheck2(input, s"Map(2 -> ${5 / 2}, 1 -> ${7 / 2})", expectedType)
  }

  it should "process bool keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").bool_int
    val expectedType = "MapType(BooleanType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(false -> 5)", expectedType)
    avgAndCheck2(input, s"Map(false -> ${5 / 2})", expectedType)
  }

  it should "process date keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").date_int
    val expectedType = "MapType(DateType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(2021-05-12 -> 5)", expectedType)
    avgAndCheck2(input, s"Map(2021-05-12 -> ${5 / 2})", expectedType)
  }

  it should "process time keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").time_int
    val expectedType = "MapType(TimestampType,IntegerType,true)"
    show(input, message = "input")
    sumAndCheck2(input, "Map(2021-05-12 12:34:55.0 -> 5)", expectedType)
    avgAndCheck2(input, s"Map(2021-05-12 12:34:55.0 -> ${5 / 2})", expectedType)
  }

}
