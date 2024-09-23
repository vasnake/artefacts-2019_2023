/**
 * Created by vasnake@gmail.com on 2024-09-19
 */
package com.github.vasnake.spark.dataset.transform

import com.github.vasnake.spark.test._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql
import sql._

import org.scalatest.flatspec._
import org.scalatest.matchers._

// testOnly *SampleFastTest* -- -z "smoke"
class SampleFastTest extends AnyFlatSpec with should.Matchers with LocalSpark with DataFrameHelpers {
  val rowsCount = 20
  val partsCount = 4

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.range(rowsCount)
      .selectExpr("id")
      .orderBy("id")
      .repartition(partsCount)
      .persist(StorageLevel.DISK_ONLY) // saveAsTable, parquet: not working w/o hadoop. Only in IT or E2E env.
      .createOrReplaceTempView("t1")
  }

  override def afterAll(): Unit = {
    spark.catalog.dropTempView("t1")
    super.afterAll()
  }

  it should "perform smoke test" in {
    import spark.implicits._

    val sampleSize: Int = 3
    val expected: Array[Long] = Array(5, 9, 13, 18) // first partition, all its rows

    val df = spark.sql("SELECT id FROM t1")
    show(df, message = "source")

    val sampleDS: Dataset[Long] = SampleFast.apply(df, sampleSize)
      .as[Long]

    val actual = sampleDS.collect()

    actual should contain theSameElementsAs expected
  }

  it should "select exact number of records" in {
    import spark.implicits._

    val sampleSize: Int = 3
    val expected: Array[Long] = Array(5, 9, 13) // 'limit(3)' was added

    val df = spark.sql("SELECT id FROM t1")

    val sampleDS: Dataset[Long] = SampleFast.apply(df, sampleSize)
      .as[Long]

    val actual = sampleDS.limit(sampleSize).collect()

    actual should contain theSameElementsAs expected
  }

  it should "touch only one partition" in {
    import spark.implicits._

    val sampleSize: Int = 3 // first partition size = 4

    val accum: LongAccumulator = spark.sparkContext.longAccumulator("Rows touched")
    val processOneRow: Long => Long = { v => {
      accum.add(1) // println(s"ROW ID: $v")
      v
    }}

    val df = spark.sql("SELECT id FROM t1")
      .as[Long]
      .map(processOneRow)

    val sampleDS: Dataset[Long] = SampleFast.apply(df.toDF, sampleSize) // first action
      .as[Long]

    assert(accum.value == sampleSize) // rows touched while counting num partitions that we need

    val actual = sampleDS.collect() // second action

    assert(accum.value == (sampleSize + 4)) // rows touched before + first partition size
    assert(actual.length >= sampleSize)

  }

  // Only base case checked, TODO: check more-than-one partition; check corner cases
}
