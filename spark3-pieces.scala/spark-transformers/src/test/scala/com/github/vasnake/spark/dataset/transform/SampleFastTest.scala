/**
 * Created by vasnake@gmail.com on 2024-09-19
 */
package com.github.vasnake.spark.dataset.transform

import com.github.vasnake.spark.test._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql
import sql._

import org.scalatest.flatspec._
import org.scalatest.matchers._

// testOnly *SampleFastTest* -- -z "smoke"
class SampleFastTest extends AnyFlatSpec with should.Matchers with LocalSpark with DataFrameHelpers {

  it should "perform smoke test" in {
    import spark.implicits._

    val rowsCount = 20
    val partsCount = 4
    val sampleSize: Int = 3
    val expected: Array[Long] = Array(5, 9, 13, 18) // first partition
    // val expected: Array[Long] = Array(5, 9, 13) // if 'limit' was added

    spark.range(rowsCount)
      .selectExpr("id")
      .persist(StorageLevel.DISK_ONLY) // saveAsTable, parquet: not working w/o hadoop. Only in IT env.
    .createOrReplaceTempView("t1")

    val df = spark.sql("SELECT id FROM t1")
      .orderBy("id")
      .repartition(partsCount)
    show(df, message = "source")

    val sampleDS: Dataset[Long] = SampleFast.approxLazyLimit(df, sampleSize)
      .as[Long]

    val actual = sampleDS.collect()
    // val actual = sampleDS.limit(sampleSize).collect()

    actual should contain theSameElementsAs expected
  }

}
