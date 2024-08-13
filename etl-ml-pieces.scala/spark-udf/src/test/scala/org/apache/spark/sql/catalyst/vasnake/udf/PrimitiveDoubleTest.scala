/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql

import com.github.vasnake.spark.test.LocalSpark

class PrimitiveDoubleTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  import sql.DataFrame
  import Fixtures._
  import functions.generic_sum

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", "cast(feature as double) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "DoubleType"

  // TODO: add tests for min/max/avg/most_freq API's
  it should "pass smoke test, imperative sum" in {
    val input = inputDF.where("part = 'A'")
    show(input, message = "input")

    // DataFrame API
    def nativeCall = input.groupBy("part").agg(sql.Column(GenericSum(sql.Column("feature").expr).toAggregateExpression))
    val output = input.groupBy("part").agg(generic_sum("feature"))
    output.explain(extended = true)
    show(output, message = "generic imperative sum output, DataFrame API", force = true)
    show(nativeCall)

    // SQL API
    functions.registerAs("generic_sum", "generic_sum", spark, overrideIfExists = true)
    input.createOrReplaceTempView("features")
    show(
      spark.sql("select part, generic_sum(feature) from features group by part"),
      message = "generic_sum output, SQL API",
      force = true
    )
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
    mfqAndCheckFull(input, expected = "0.0", threshold = "2/3") // freq >= threshold, have good value
    mfqAndCheckFull(input, expected = "null", threshold = "3/4") // freq < threshold, no good values
  }

  it should "select value by index if there is a tie, part M, most_freq" in {
    val input = inputDF.where("part = 'M'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0.0", index = "1")
    mfqAndCheckFull(input, expected = "1.0", index = "0")
    mfqAndCheckFull(input, expected = "42.0", prefer = "42.0", index = "0", threshold = "1") // prefer exists
  }

}
