/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import com.github.vasnake.spark.test.DataFrameHelpers
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertion

trait Checks extends DataFrameHelpers {
  import functions.{ generic_sum, generic_min, generic_max, generic_avg, generic_most_freq }

  def resultColumnType: String

  def check: (String, String, String) => DataFrame => Assertion =
    (
      expected,
      colname,
      msg
    ) => withUnpersist(assertResult(colname, resultColumnType, msg)(_, expected))

  def check2: (String, String, String, String) => DataFrame => Assertion =
    (
      expectedData,
      expectedType,
      colname,
      msg
    ) => withUnpersist(assertResult(colname, expectedType, msg)(_, expectedData))

  val sumAndCheck: (DataFrame, String) => Assertion =
    (input, expected) =>
      check(expected, "sum", "sum output")(
        cache(input.groupBy("part").agg(generic_sum("feature") as "sum"))
      )

  val minAndCheck: (DataFrame, String) => Assertion =
    (input, expected) =>
      check(expected, "min", "min output")(
        cache(input.groupBy("part").agg(generic_min("feature") as "min"))
      )

  val maxAndCheck: (DataFrame, String) => Assertion =
    (input, expected) =>
      check(expected, "max", "max output")(
        cache(input.groupBy("part").agg(generic_max("feature") as "max"))
      )

  val avgAndCheck: (DataFrame, String) => Assertion =
    (input, expected) =>
      check(expected, "avg", "avg output")(
        cache(input.groupBy("part").agg(generic_avg("feature") as "avg"))
      )

  val sumAndCheck2: (DataFrame, String, String) => Assertion =
    (
      input,
      expectedData,
      expectedType
    ) =>
      check2(expectedData, expectedType, "sum", "sum output")(
        cache(input.groupBy("part").agg(generic_sum("feature") as "sum"))
      )

  val avgAndCheck2: (DataFrame, String, String) => Assertion =
    (
      input,
      expectedData,
      expectedType
    ) =>
      check2(expectedData, expectedType, "avg", "avg output")(
        cache(input.groupBy("part").agg(generic_avg("feature") as "avg"))
      )

  def mfqAndCheck(inp: DataFrame, expected: String): Assertion =
    mfqAndCheck2(inp, expected, resultColumnType)

  val mfqAndCheck2: (DataFrame, String, String) => Assertion =
    (
      input,
      expectedData,
      expectedType
    ) =>
      check2(expectedData, expectedType, "mfq", "mfq output")(
        cache(input.groupBy("part").agg(generic_most_freq("feature") as "mfq"))
      )

  def assertResult(
    colname: String,
    coltype: String,
    msg: String
  )(
    actual: DataFrame,
    expected: String
  ): Assertion = {
    show(actual, msg)
    assert(actual.schema(colname).toString() === s"StructField(${colname},${coltype},true)")
    assert(actual.select(colname).collect().map(_.toString()).toList === List(s"[${expected}]"))
  }

  def mfqAndCheckFull(
    inp: DataFrame,
    expected: String,
    index: String = "null",
    threshold: String = "null",
    prefer: String = "null"
  ): Assertion = {
    import functions.generic_most_freq

    check2(expected, resultColumnType, "mfq", "mfq output")(
      cache(
        inp
          .groupBy("part")
          .agg(
            generic_most_freq(columnName = "feature", index, threshold, prefer) as "mfq"
          )
      )
    )
  }
}
