/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql.DataFrame

import com.github.vasnake.spark.test.LocalSpark

class MapDoubleTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", "cast(feature_map as map<string, double>) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "MapType(StringType,DoubleType,true)"

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
    avgAndCheck(input, "Map(2 -> null, 1 -> 3.5)")
  }

  it should "ignore invalid if valid values exists, part I" in {
    val input = inputDF.where("part = 'I'")
    show(input, message = "input")
    sumAndCheck(input, "Map(2 -> 5.0, 1 -> 7.0)")
    minAndCheck(input, "Map(2 -> 2.0, 1 -> 3.0)")
    maxAndCheck(input, "Map(2 -> 3.0, 1 -> 4.0)")
    avgAndCheck(input, "Map(2 -> 2.5, 1 -> 3.5)")
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
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").int_double
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5.0, 1 -> 7.0)", "MapType(IntegerType,DoubleType,true)")
    avgAndCheck2(input, "Map(2 -> 2.5, 1 -> 3.5)", "MapType(IntegerType,DoubleType,true)")
  }

  it should "process long keys, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").long_double
    show(input, message = "input")
    sumAndCheck2(input, "Map(2 -> 5.0, 1 -> 7.0)", "MapType(LongType,DoubleType,true)")
    avgAndCheck2(input, "Map(2 -> 2.5, 1 -> 3.5)", "MapType(LongType,DoubleType,true)")
  }

  it should "avg ArrayBasedMapData, part I" in {
    val input: DataFrame = MapTransformer(inputDF, "part = 'I'").int_double
    show(input, message = "input")

    // regression test for
    // ClassCastException: org.apache.spark.sql.catalyst.util.ArrayBasedMapData cannot be cast to org.apache.spark.sql.catalyst.expressions.UnsafeMapData
    // detected in rare case when map[int,double] cast to map[string,float],
    // e.g. `gavg(cast(score as map<string,float>))` where score is map[int,double]

    functions.registerAs("generic_avg", "gavg", spark, overrideIfExists = true)
    import org.apache.spark.sql.functions.expr

    show(
      input.groupBy("part").agg(expr("gavg(cast(feature as map<string,float>))")),
      message = "avg result",
      force = true
    )

  }

}
