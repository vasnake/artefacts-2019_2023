/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark
import Fixtures._
import functions._

import org.apache.spark.sql
import sql.DataFrame
import sql.catalyst.expressions.Expression

//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class PrimitiveDoubleTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", "cast(feature as double) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "DoubleType"

  // testOnly *PrimitiveDoubleTest* -- -z "smoke"
  it should "pass smoke test, generic sum" in {
    functions.registerAs("generic_sum", "generic_sum", spark, overrideIfExists = true)
    val input = inputDF.where("part = 'A'")
    input.createOrReplaceTempView("features")
    show(input, message = "input")

    show(
      input.groupBy("part").agg(sql.Column(GenericSum(sql.Column("feature").expr).toAggregateExpression)),
      message = "generic_sum output, DataFrame API, native",
      force = true
    )

    show(
      input.groupBy("part").agg(generic_sum("feature")),
      message = "generic_sum output, DataFrame API, shortcut",
      force = true
    )

    show(
      spark.sql("select part, generic_sum(feature) from features group by part"),
      message = "generic_sum output, SQL API",
      force = true
    )
  }

  it should "pass smoke test, generic min/max/avg/most_freq" in {
    val input = inputDF.where("part = 'A'")
    input.createOrReplaceTempView("features")
    show(input, message = "input")

    def c(s: String):Expression = sql.Column(s).expr
    def e(s: String): Expression = sql.functions.expr(s).expr

    val funcsTable: Seq[(String, Expression, String => sql.Column)] = Seq(
      ("generic_min", GenericMin(c("feature")).toAggregateExpression, generic_min),
      ("generic_max", GenericMax(c("feature")).toAggregateExpression, generic_max),
      ("generic_avg", GenericAvg(c("feature")).toAggregateExpression, generic_avg),
      (
        "generic_most_freq",
        GenericMostFreq(c("feature"), e("null"), e("null"), e("null")).toAggregateExpression,
        s => generic_most_freq(s, "null", "null", "null")
      )
    )

    for {
      (fName, aggregateExpr, aggregateFun) <- funcsTable
    } yield {
      functions.registerAs(fName, fName, spark, overrideIfExists = true)
      show(
        input.groupBy("part").agg(sql.Column(aggregateExpr)),
        message = s"$fName output, DataFrame API, native",
        force = true
      )
      show(
        input.groupBy("part").agg(aggregateFun("feature")),
        message = s"$fName output, DataFrame API, shortcut",
        force = true
      )

      show(
        spark.sql(s"select part, $fName(feature) from features group by part"),
        message = s"$fName output, SQL API",
        force = true
      )
    }
  }

  it should "produce null if all values are null, part B" in {
    val input = inputDF.where("part = 'B'")
    show(input, message = "input")
    sumAndCheck(input, "null")
    minAndCheck(input, "null")
    maxAndCheck(input, "null")
    avgAndCheck(input, "null")
    mfqAndCheck(input, "null")
  }

  it should "ignore invalid if valid values exists, part D" in {
    val input = inputDF.where("part = 'D'")
    show(input, message = "input")
    sumAndCheck(input, "7.0")
    minAndCheck(input, "0.0")
    maxAndCheck(input, "4.0")
    avgAndCheck(input, "1.75")
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
}
