/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark
import org.apache.spark.sql
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class PrimitiveFloatTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {
  import sql.DataFrame
  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .selectExpr("part", "uid", "cast(feature as float) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "FloatType"

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
    mfqAndCheck(input, "0.0") // most_freq works with nan
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
