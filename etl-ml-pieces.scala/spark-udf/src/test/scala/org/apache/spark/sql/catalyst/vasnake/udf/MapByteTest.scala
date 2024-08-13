/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql.DataFrame

import com.github.vasnake.spark.test.LocalSpark

class MapByteTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

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
      .selectExpr("part", "uid", "cast(feature_map as map<string, byte>) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "MapType(StringType,ByteType,true)"

  it should "produce null item if all items are invalid, part G" in {
    val input = inputDF.where("part = 'G'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> null, 1 -> 7)")
    minAndCheck(input, "Map(2 -> null, 1 -> 3)")
    maxAndCheck(input, "Map(2 -> null, 1 -> 4)")
    avgAndCheck(input, "Map(2 -> null, 1 -> 3)")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> 5, 1 -> 7)")
    minAndCheck(input, "Map(2 -> 2, 1 -> 3)")
    maxAndCheck(input, "Map(2 -> 3, 1 -> 4)")
    avgAndCheck(input, "Map(2 -> 2, 1 -> 3)")
  }

  it should "process string keys, part K" in {
    val input = inputDF.where("part = 'K'")
    show(input, message = "input")
    sumAndCheck(input, "Map(foo -> 1, 1 -> 0)")
    minAndCheck(input, "Map(foo -> 0, 1 -> 0)")
    maxAndCheck(input, "Map(foo -> 1, 1 -> 0)")
    avgAndCheck(input, "Map(foo -> 0, 1 -> 0)")
  }

  it should "process byte keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").byte_byte
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5, 1 -> 7)", "MapType(ByteType,ByteType,true)")
    avgAndCheck2(input, "Map(2 -> 2, 1 -> 3)", "MapType(ByteType,ByteType,true)")
  }

  it should "process short keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").short_byte
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5, 1 -> 7)", "MapType(ShortType,ByteType,true)")
    avgAndCheck2(input, "Map(2 -> 2, 1 -> 3)", "MapType(ShortType,ByteType,true)")
  }

}
