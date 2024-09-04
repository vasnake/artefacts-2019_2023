/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import scala.collection.mutable

import com.github.vasnake.spark.test.LocalSpark
import org.apache.spark.sql
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class PrimitiveStringTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {
  import sql.DataFrame
  import sql.{ functions => sqlfn }
  import Fixtures._
  import functions.generic_most_freq

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .where(
        "feature is null or feature not in (double('NaN'), double('-Infinity'), double('Infinity'))"
      )
      .selectExpr("part", "uid", "cast(feature as string) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "StringType"

  it should "pass smoke test, most_freq native API" in {
    val input = inputDF.where("part = 'A'")
    show(input, message = "input")

    show(
      input
        .groupBy("part")
        .agg(
          sql.Column(
            new GenericMostFreq(sql.Column("feature").expr).toAggregateExpression
          )
        ),
      message = "most_freq, native, short",
      force = true
    )

    show(
      input
        .groupBy("part")
        .agg(
          sql.Column(
            new GenericMostFreq(
              sql.Column("feature").expr,
              indexExpression = sqlfn.lit(0).expr,
              thresholdExpression = sqlfn.lit(null).expr,
              preferExpression = sqlfn.lit("42").expr
            ).toAggregateExpression
          )
        ),
      message = "most_freq, native, full",
      force = true
    )
  }

  it should "pass smoke test, most_freq DataFrame API" in {
    val input = inputDF.where("part = 'A'")
    show(input, message = "input")

    show(
      input
        .groupBy("part")
        .agg(
          functions.generic_most_freq(
            sqlfn.col("feature"),
            index = sqlfn.lit(0).expr,
            threshold = sqlfn.lit(0.1).expr,
            prefer = sqlfn.lit("42").expr
          )
        ),
      message = "gmf, DataFrame API shortcut",
      force = true
    )

    val output = input.groupBy("part").agg(functions.generic_most_freq("feature"))
    output.explain(extended = true)
    show(
      output,
      message = "generic imperative most_freq output, DataFrame API shortcut 2",
      force = true
    )
  }

  it should "pass smoke test, most_freq SQL API" in {
    val input = inputDF.where("part = 'A'")
    show(input, message = "input")

    functions.registerAs("generic_most_freq", "gmf", spark, overrideIfExists = true)
    input.createOrReplaceTempView("features")

    show(
      spark.sql("select part, gmf(feature) from features group by part"),
      message = "most_freq, SQL API, short",
      force = true
    )

    val index = "0"
    val threshold = "0.1"
    val prefer = "1"
    show(
      spark.sql(
        s"select part, gmf(feature, ${index}, ${threshold}, ${prefer}) from features group by part"
      ),
      message = "most_freq, SQL API, full",
      force = true
    )

    show(
      input.groupBy("part").agg(sqlfn.expr("gmf(feature)")),
      message = "generic_most_freq, short expr"
    )

    show(
      input.groupBy("part").agg(sqlfn.expr("gmf(feature, 0, 0.1, '42')")),
      message = "gmf, full expr"
    )
  }

  it should "produce null if all values are invalid, part B, most_freq" in {
    val input = inputDF.where("part = 'B'")
    show(input, message = "input")
    mfqAndCheck(input, "null")
  }

  it should "ignore invalid if valid values exists, part D, most_freq" in {
    val input = inputDF.where("part = 'D'")
    show(input, message = "input")
    mfqAndCheck(input, "0.0")
  }

  it should "select most frequent valid item, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheck(input, "0.0")
  }

  it should "select prefer value, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "1.0", prefer = "1.0") // prefer exists
    mfqAndCheckFull(input, expected = "1.0", threshold = "1", prefer = "1.0") // prefer exists, ignore threshold
    mfqAndCheckFull(input, expected = "0.0", prefer = "42") // no such prefer, return most frequent
  }

  it should "filter with threshold, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0.0", threshold = "2.0/3.0001") // freq >= threshold, have good value
    mfqAndCheckFull(input, expected = "null", threshold = "2.0001/3.0") // freq < threshold, no good values
  }

  it should "select value by index if there is a tie, part M, most_freq" in {
    val input = inputDF.where("part = 'M'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0.0", index = "1")
    mfqAndCheckFull(input, expected = "1.0", index = "0")
    mfqAndCheckFull(input, expected = "42.0", prefer = "42.0", index = "0", threshold = "1") // prefer exists
  }

  it should "select value by random index if there is a tie, part M, most_freq" in {
    val input = cache(inputDF.where("part = 'M'"))
    show(input, message = "input")

    val set = mutable.Set.empty[String]
    var iterNo: Int = 0
    while (set.size < 2 && iterNo < 1000) {
      iterNo += 1
      set += input
        .groupBy("part")
        .agg(generic_most_freq("feature") as "mfq")
        .head()
        .getAs[String]("mfq")
    }
    assert(set.toList.sorted === List("0.0", "1.0"))
  }
}
