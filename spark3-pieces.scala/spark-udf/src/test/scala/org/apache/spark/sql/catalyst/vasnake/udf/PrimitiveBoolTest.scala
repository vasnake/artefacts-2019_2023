/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark
import org.apache.spark.sql
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class PrimitiveBoolTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {
  import sql.DataFrame
  import sql.{ functions => sqlfn }
  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr(
        "part",
        "uid",
        "cast(if(feature in (null, 'NaN', 'Infinity', '-Infinity'), null, feature) as boolean) as feature"
      )
      .repartition(4)
  )

  val resultColumnType: String = "BooleanType"

  it should "pass smoke test, most_freq" in {
    val input = inputDF.where("part = 'A'")
    show(input, message = "input")

    // SQL API

    functions.registerAll(spark, overrideIfExists = true)
    functions.registerAs("generic_most_freq", "gmf", spark, overrideIfExists = true)
    functions.registerAs("generic_most_freq", "generic_most_freq", spark, overrideIfExists = true)
    input.createOrReplaceTempView("features")

    show(
      cache(spark.sql("select part, generic_most_freq(feature) from features group by part")),
      message = "most_freq, SQL API, short",
      force = true
    )

    val rnd = "cast(0 as int)"
    val threshold = "cast(null as float)"
    val prefer = "cast(0 as boolean)"
    show(
      cache(
        spark.sql(
          s"select part, gmf(feature, ${rnd}, ${threshold}, ${prefer}) from features group by part"
        )
      ),
      message = "most_freq, SQL API, full",
      force = true
    )

    // DataFrame API

    def shortNativeCall = input
      .groupBy("part")
      .agg(
        sql.Column(
          new GenericMostFreq(sql.Column("feature").expr).toAggregateExpression
        )
      )

    def fullNativeCall = input
      .groupBy("part")
      .agg(
        sql.Column(
          new GenericMostFreq(
            sql.Column("feature").expr,
            indexExpression = sqlfn.lit(0).cast("int").expr,
            thresholdExpression = sqlfn.lit(null).cast("float").expr,
            preferExpression = sqlfn.lit(false).expr
          ).toAggregateExpression
        )
      )

    show(cache(shortNativeCall), message = "most_freq, native, short")
    show(cache(fullNativeCall), message = "most_freq, native, full")

    val output = input.groupBy("part").agg(functions.generic_most_freq("feature"))
    output.explain(extended = true)
    show(
      cache(output),
      message = "generic imperative most_freq output, DataFrame API shortcut",
      force = true
    )

    show(
      cache(input.groupBy("part").agg(sqlfn.expr("generic_most_freq(feature)"))),
      message = "generic_most_freq, expr"
    )

    show(
      cache(input.groupBy("part").agg(sqlfn.expr("gmf(feature, 0, null, false)"))),
      message = "gmf, expr"
    )
  }

  it should "produce null if all values are null, part B" in {
    val input = inputDF.where("part = 'B'")
    show(input, message = "input")
    mfqAndCheck(input, "null")
  }

  it should "ignore invalid if valid values exists, part D" in {
    val input = inputDF.where("part = 'D' and uid != 'c'")
    show(input, message = "input")
    mfqAndCheck(input, "true")
  }

  it should "select most frequent valid item, part L" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheck(input, "false")
  }
}
