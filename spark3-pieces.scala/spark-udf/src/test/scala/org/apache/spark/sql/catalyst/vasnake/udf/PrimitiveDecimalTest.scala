/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.LocalSpark
import org.apache.spark.sql
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class PrimitiveDecimalTest extends AnyFlatSpec with should.Matchers with LocalSpark with Checks {
  import sql.DataFrame
  import Fixtures._

  lazy val inputDF: DataFrame = cache(
    createInputDF(spark)
      .where("feature is null or feature not in ('NaN', '-Infinity', 'Infinity')")
      .selectExpr("part", "uid", "cast(feature as decimal(4,3)) as feature")
      .repartition(4)
  )

  val resultColumnType: String = "DecimalType(14,3)"

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
    sumAndCheck(input, "7.000")
    minAndCheck(input, "0.000")
    maxAndCheck(input, "4.000")
    avgAndCheck(input, "1.750")
    mfqAndCheckFull(input, "0.000", index = "0")
  }

  it should "select most frequent valid item, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheck(input, "0.000")
  }

  it should "select prefer value, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "1.000", prefer = "1.000") // prefer exists
    mfqAndCheckFull(input, expected = "1.000", threshold = "1", prefer = "1.000") // prefer exists, ignore threshold
    mfqAndCheckFull(input, expected = "0.000", prefer = "42") // no such prefer, return most frequent
  }

  it should "filter with threshold, part L, most_freq" in {
    val input = inputDF.where("part = 'L'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0.000", threshold = "2.0/3.0001") // freq >= threshold, have good value
    mfqAndCheckFull(input, expected = "null", threshold = "2.0001/3.0") // freq < threshold, no good values
  }

  it should "select value by index if there is a tie, part M, most_freq" in {
    val input = inputDF.where("part = 'M'")
    show(input, message = "input")
    mfqAndCheckFull(input, expected = "0.000", index = "1")
    mfqAndCheckFull(input, expected = "1.000", index = "0")
    mfqAndCheckFull(input, expected = "4.200", prefer = "4.200", index = "0", threshold = "1") // prefer exists
  }
}
