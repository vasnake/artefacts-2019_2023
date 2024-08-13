/**
 * Created by vasnake@gmail.com on 2024-08-13
 */
package org.apache.spark.sql.catalyst.vasnake.udf

import org.scalatest._
import flatspec._
import matchers._

import org.apache.spark.sql

import com.github.vasnake.spark.test.LocalSpark

class PrimitiveByteTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {

  import sql.DataFrame
  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .where("feature is null or feature not in ('NaN', '-Infinity', 'Infinity')")
      .selectExpr("part", "uid", "cast(feature as byte) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "ByteType"

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
    sumAndCheck(input, "7")
    minAndCheck(input, "0")
    maxAndCheck(input, "4")
    avgAndCheck(input, ((4 + 3) / 5).toString)
    mfqAndCheck(input, "0")
  }

  it should "select most frequent valid item, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheck(input, "0")
  }

  it should "select prefer value, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "1", prefer = "1") // prefer exists
    mfqAndCheckFull(input, expected = "1", threshold = "1", prefer = "1") // prefer exists, ignore threshold
    mfqAndCheckFull(input, expected = "0", prefer = "42") // no such prefer, return most frequent
  }

  it should "filter with threshold, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0", threshold = "2/3") // freq >= threshold, have good value
    mfqAndCheckFull(input, expected = "null", threshold = "3/4") // freq < threshold, no good values
  }

  it should "select value by index if there is a tie, part M, most_freq" in {
    val input = inputDF.where("part = 'M'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0", index = "1")
    mfqAndCheckFull(input, expected = "1", index = "0")
    mfqAndCheckFull(input, expected = "42", prefer = "42", index = "0", threshold = "1") // prefer exists
  }

}