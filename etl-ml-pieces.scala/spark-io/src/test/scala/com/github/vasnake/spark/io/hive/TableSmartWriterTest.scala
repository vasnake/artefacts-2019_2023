/**
 * Created by vasnake@gmail.com on 2024-08-09
 */
package com.github.vasnake.spark.io.hive

import com.github.vasnake.spark.test.LocalSpark

import org.scalatest._
import flatspec._
import matchers._

class TableSmartWriterTest extends AnyFlatSpec with should.Matchers with PrivateMethodTester with LocalSpark {

  it should "produce pool size parameter value" in {
    // private def _poolSizeParameterValue(spark: SparkSession, key: String, default: Int)
    val key = TableSmartWriter.METASTORE_CONNECTIONS_POOL_SIZE_KEY
    val _poolSizeParameterValue = PrivateMethod[Int]('_poolSizeParameterValue)

    // default
    assert(TableSmartWriter.invokePrivate(_poolSizeParameterValue(spark, key, 7)) === 7)

    spark.conf.set(key, "3")
    assert(TableSmartWriter.invokePrivate(_poolSizeParameterValue(spark, key, 7)) === 3)

    // invalid fallback to default
    spark.conf.set(key, "abc")
    assert(TableSmartWriter.invokePrivate(_poolSizeParameterValue(spark, key, 7)) === 7)
  }

  it should "compute pool size" in {
    // _poolSize(maxSize: Int, partitionsTotalCount: Int, chunkSize: Int)
    val _poolSize = PrivateMethod[Int]('_poolSize)
    Seq(
      // (poolMaxSize, partsTotalCount, chunkSize, expected)
      (8, 99000, 150, 8),
      (8, 1500, 150, 8),
      (8, 1050, 150, 7),
      (8, 301, 150, 3),
      (8, 300, 150, 2),
      (8, 151, 150, 2),
      (8, 150, 150, 1),
      (8, 149, 150, 1),
      (8, 7, 3, 3),
      (2, 7, 3, 2),
      (1, 7, 3, 1),
    ).foreach { case (maxSize, partsTotalCount, chunkSize, expected) =>
      assert(TableSmartWriter.invokePrivate(_poolSize(maxSize, partsTotalCount, chunkSize)) === expected)
    }

    assertThrows[AssertionError] {
      TableSmartWriter.invokePrivate(_poolSize(-1, 7, 3))
    }
  }

}
