/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql.DataFrame

import com.github.vasnake.spark.test.LocalSpark

class MapFloatTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", "cast(feature_map as map<string, float>) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "MapType(StringType,FloatType,true)"

  it should "produce null if all collections are null, part E" in {
    val input = inputDF.where("part = 'E'")
    show(input, message = "input")
    sumAndCheck(input, "null")
    minAndCheck(input, "null")
    maxAndCheck(input, "null")
    avgAndCheck(input, "null")
  }

  it should "produce empty collection if all collections are null or empty, part F" in {
    val input = inputDF.where("part = 'F'")
    show(input, message = "input")
    sumAndCheck(input, "Map()")
    minAndCheck(input, "Map()")
    maxAndCheck(input, "Map()")
    avgAndCheck(input, "Map()")
  }

  it should "produce null item if all items are invalid, part G" in {
    val input = inputDF.where("part = 'G'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> null, 1 -> 7.0)")
    minAndCheck(input, "Map(2 -> null, 1 -> 3.0)")
    maxAndCheck(input, "Map(2 -> null, 1 -> 4.0)")
    avgAndCheck(input, s"Map(2 -> null, 1 -> ${7.0 / 2})")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> 5.0, 1 -> 7.0)")
    minAndCheck(input, "Map(2 -> 2.0, 1 -> 3.0)")
    maxAndCheck(input, "Map(2 -> 3.0, 1 -> 4.0)")
    avgAndCheck(input, s"Map(2 -> ${5.0 / 2}, 1 -> ${7.0 / 2})")
  }

  it should "consider absent values as null, part J" in {
    val input = inputDF.where("part = 'J'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> 2.0, 3 -> 3.0, 1 -> 1.0)")
    minAndCheck(input, "Map(2 -> 2.0, 3 -> 3.0, 1 -> 1.0)")
    maxAndCheck(input, "Map(2 -> 2.0, 3 -> 3.0, 1 -> 1.0)")
    avgAndCheck(input, "Map(2 -> 2.0, 3 -> 3.0, 1 -> 1.0)")
  }

  it should "process string keys, part K" in {
    val input = inputDF.where("part = 'K'")
    show(input, message = "input")
    sumAndCheck(input, "Map(foo -> 2.0, 1 -> 1.0)")
    minAndCheck(input, "Map(foo -> 0.3, 1 -> 0.5)")
    maxAndCheck(input, "Map(foo -> 1.7, 1 -> 0.5)")
    avgAndCheck(input, "Map(foo -> 1.0, 1 -> 0.5)")
  }

  it should "process int keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").int_float
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5.0, 1 -> 7.0)", "MapType(IntegerType,FloatType,true)")
    avgAndCheck2(input, "Map(2 -> 2.5, 1 -> 3.5)", "MapType(IntegerType,FloatType,true)")
  }

  it should "process long keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").long_float
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5.0, 1 -> 7.0)", "MapType(LongType,FloatType,true)")
    avgAndCheck2(input, "Map(2 -> 2.5, 1 -> 3.5)", "MapType(LongType,FloatType,true)")
  }

}
