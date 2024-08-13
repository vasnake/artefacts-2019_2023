/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.hive.vasnake

import scala.math.Integral.Implicits.infixIntegralOps

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class MetastoreQueryProcessorWithConnPoolTest extends AnyFlatSpec with should.Matchers {
  import MetastoreQueryProcessorWithConnPool.sliceIndices

  it should "maintain invariants" in {
    // testOnly *MetastoreQueryProcessorWithConnPoolTest* -- -z "invariants"
    // - concatenated collection of slices should be equal to original set of indices;
    // - length of each slice should be numPartItems <= x <= numPartItems + 1

    def test(totalItems: Int, numParts: Int): Unit = {
      val allIndices: Array[Int] = (0 until totalItems).toArray
      val (numPartItems, reminder) = totalItems /% numParts

      val slicedIndices: Seq[Seq[Int]] =
        (0 until numParts).map(partIdx => sliceIndices(numPartItems, reminder, partIdx, totalItems))

      assert(
        slicedIndices.forall(slice =>
          slice.length == numPartItems || slice.length == numPartItems + 1
        )
      )

      val flatten = slicedIndices.flatten.toArray
      assert(flatten.length == totalItems)
      flatten should contain theSameElementsAs allIndices
    }

    (12 to 1234) map (totalItems => test(totalItems, numParts = 12))
  }

  it should "slice array indices" in {
    // testOnly *MetastoreQueryProcessorWithConnPoolTest* -- -z "slice array indices"
    // val (quotient, remainder) = dividend /% divisor

    // def _sliceIndices(numItems: Int, reminder: Int, sliceIdx: Int, totalItems: Int): Seq[Int]

    // 3 /% 2 = (1,1)
    sliceIndices(1, 1, 0, 3) should contain theSameElementsAs Seq(0, 2)
    sliceIndices(1, 1, 1, 3) should contain theSameElementsAs Seq(1)

    // 2 /% 2 = (1,0)
    sliceIndices(1, 0, 0, 2) should contain theSameElementsAs Seq(0)
    sliceIndices(1, 0, 1, 2) should contain theSameElementsAs Seq(1)

    // 4 /% 2 = (2,0)
    sliceIndices(2, 0, 0, 4) should contain theSameElementsAs Seq(0, 1)
    sliceIndices(2, 0, 1, 4) should contain theSameElementsAs Seq(2, 3)

    // 5 /% 2 = (2,1)
    sliceIndices(2, 1, 0, 5) should contain theSameElementsAs Seq(0, 1, 4)
    sliceIndices(2, 1, 1, 5) should contain theSameElementsAs Seq(2, 3)

    // 3 /% 3 = (1,0)
    sliceIndices(1, 0, 0, 3) should contain theSameElementsAs Seq(0)
    sliceIndices(1, 0, 1, 3) should contain theSameElementsAs Seq(1)
    sliceIndices(1, 0, 2, 3) should contain theSameElementsAs Seq(2)

    // 4 /% 3 = (1,1)
    sliceIndices(1, 1, 0, 4) should contain theSameElementsAs Seq(0, 3)
    sliceIndices(1, 1, 1, 4) should contain theSameElementsAs Seq(1)
    sliceIndices(1, 1, 2, 4) should contain theSameElementsAs Seq(2)

    // 5 /% 3 = (1,2)
    sliceIndices(1, 2, 0, 5) should contain theSameElementsAs Seq(0, 4)
    sliceIndices(1, 2, 1, 5) should contain theSameElementsAs Seq(1, 3)
    sliceIndices(1, 2, 2, 5) should contain theSameElementsAs Seq(2)

    // 6 /% 3 = (2,0)
    sliceIndices(2, 0, 0, 6) should contain theSameElementsAs Seq(0, 1)
    sliceIndices(2, 0, 1, 6) should contain theSameElementsAs Seq(2, 3)
    sliceIndices(2, 0, 2, 6) should contain theSameElementsAs Seq(4, 5)

    // 7 /% 3 = (2,1)
    sliceIndices(2, 1, 0, 7) should contain theSameElementsAs Seq(0, 1, 6)
    sliceIndices(2, 1, 1, 7) should contain theSameElementsAs Seq(2, 3)
    sliceIndices(2, 1, 2, 7) should contain theSameElementsAs Seq(4, 5)

    // 8 /% 3 = (2,2)
    sliceIndices(2, 2, 0, 8) should contain theSameElementsAs Seq(0, 1, 7)
    sliceIndices(2, 2, 1, 8) should contain theSameElementsAs Seq(2, 3, 6)
    sliceIndices(2, 2, 2, 8) should contain theSameElementsAs Seq(4, 5)

    // scala> 119 /% 12 // res12: (Int, Int) = (9,11)
    sliceIndices(9, 11, 0, 119) should contain theSameElementsAs Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 118)
    sliceIndices(9, 11, 1, 119) should contain theSameElementsAs Seq(9, 10, 11, 12, 13, 14, 15, 16,
      17, 117)
    sliceIndices(9, 11, 2, 119) should contain theSameElementsAs Seq(18, 19, 20, 21, 22, 23, 24, 25,
      26, 116)
    // ...
    sliceIndices(9, 11, 10, 119) should contain theSameElementsAs Seq(90, 91, 92, 93, 94, 95, 96,
      97, 98, 108)
    sliceIndices(9, 11, 11, 119) should contain theSameElementsAs Seq(99, 100, 101, 102, 103, 104,
      105, 106, 107)

    // scala> 12 /% 12 // res0: (Int, Int) = (1,0)
    sliceIndices(1, 0, 0, 12) should contain theSameElementsAs Seq(0)
    sliceIndices(1, 0, 1, 12) should contain theSameElementsAs Seq(1)
    sliceIndices(1, 0, 2, 12) should contain theSameElementsAs Seq(2)
    // ...
    sliceIndices(1, 0, 11, 12) should contain theSameElementsAs Seq(11)

    // scala> 13 /% 12 // res1: (Int, Int) = (1,1)
    sliceIndices(1, 1, 0, 13) should contain theSameElementsAs Seq(0, 12)
    sliceIndices(1, 1, 1, 13) should contain theSameElementsAs Seq(1)
    sliceIndices(1, 1, 2, 13) should contain theSameElementsAs Seq(2)
    // ...
    sliceIndices(1, 1, 11, 13) should contain theSameElementsAs Seq(11)
  }

  object ManualTests {
    private def _spark_shell_closeConnectionsTest(spark: SparkSession): Unit = {
      import sql.DataFrame
      import org.apache.spark.storage.StorageLevel
      import com.github.vasnake.spark.io.hive.{ TableSmartWriter => Writer }

      val database = "dbname"
      val table = "one_feature"
      val writer = new Writer()
      val parts_130 = (1 to 65) flatMap { idx =>
        Seq("HID", "VKID") map { uid_type =>
          (idx.toString, idx.toDouble / 1000.0, s"2022-02-${idx}", uid_type)
        }
      }
      val df: DataFrame = spark
        .createDataFrame(parts_130)
        .toDF("uid", "feature", "dt", "uid_type")
        .persist(StorageLevel.MEMORY_ONLY)
      df.count

      def _timedelta(t1: Long): String =
        f"${(System.currentTimeMillis - t1).toDouble / 1000.0}%2.2f" // https://docs.scala-lang.org/overviews/core/string-interpolation.html

      // N.B. set chunk size = 1, pool size parameter = 128, disable pool size limit,
      // than you can use num_partitions to setup parallelism

      def timeit(fun: => Unit, msg: String = "Fun time"): Unit = {
        val t1 = System.currentTimeMillis()
        fun
        println(s"${msg}: ${_timedelta(t1)} seconds")
      }

      def fun(): Unit = writer.insertIntoHive(
        df,
        database,
        table,
        maxRowsPerBucket = 37,
        overwrite = true,
        raiseOnMissingColumns = false,
        checkParameterOrNull = "test.partition.success",
      )

      // 256 iter * 32 pool will consume 8192 connections
      (1 to 3) foreach { iter =>
        println(s"\n writer.insertIntoHive, 130 partitions, iter ${iter} ...")
        timeit(fun(), s"iter ${iter} time")
      }
      // beware: OutOfMemoryError: Compressed class space
      // classloader related error, too many ExternalCatalogImpl if you recreate pool on each iteration

      // static ExternalCatalog's collection, close connections in threads and on closing pool instance
      // 12:11 started 50 iterations of 130 parts IIH
      // 13:16 ended. total connects 300 .. 400 with ups and downs
    }

    private def _spark_shell_processMetastorePartsTest(spark: SparkSession): Unit = {
      import scala.language.reflectiveCalls
      import org.apache.spark.internal.Logging
      import org.slf4j.Logger
      val logger = new Logging {
        lazy val _log: Logger = super.log
      }
      implicit val _log: Logger = logger._log

      val PARTITIONS_LIST_READ_CHUNK_SIZE = 150
      val PARTITIONS_LIST_WRITE_CHUNK_SIZE = 500
      val PARTITIONS_LIST_CONNECTIONS = 12
      val TIME_ZONE_ID = "UTC"
      val database = "dbname"
      val table = "audience_trg75523"
      val params: Map[String, Option[String]] = Map("test.write.success" -> Some("true"))
      // 32101 parts
      val partitions = spark
        .sharedState
        .externalCatalog
        .listPartitions(database, table, partialSpec = Some(Map("dt" -> "2022-01-17")))
      val dfParts: Seq[Map[String, String]] = partitions.map(_.spec)
      // 1001 parts
      val limitedPartitions = partitions.take(10000 + 1)
      val limitedDfParts = dfParts.take(10000 + 1)

      def _updateData(data: Map[String, String], updates: Map[String, Option[String]])
        : Map[String, String] =
        updates.foldLeft(data) {
          case (result, item) =>
            item match {
              case (key, None) => result - key
              case (key, Some(value)) => result + ((key, value))
            }
        }

      def _timedelta(t1: Long): String =
        f"${(System.currentTimeMillis - t1).toDouble / 1000.0}%2.2f" // https://docs.scala-lang.org/overviews/core/string-interpolation.html

      def timeit(fun: () => Unit, msg: String = "Fun time"): Unit = {
        val t1 = System.currentTimeMillis()
        fun.apply()
        println(s"${msg}: ${_timedelta(t1)} seconds")
      }

      import com.github.vasnake.spark.io.hive.TableSmartWriter.{
        _alterPartitions => ap,
        _listPartitions => lp,
      }

      def _listParts(chunk_size: Int): Unit = {
        val mqp = MetastoreQueryProcessorWithConnPool(spark, 12)
        val parts = lp(database, table, limitedDfParts, mqp)(_log, spark)
        println(s"partitions: ${parts.length}, data:\n${parts.mkString("\n").take(1000)}")
      }

      timeit(
        () => _listParts(chunk_size = 150),
        s"Query ${limitedDfParts.length} parts from metastore, time",
      )
      // pool(12) 14 sec
      // seq: chunk 150, 67 queries, 10K parts = 103 sec
      // par: chunk 150, 67 queries, 10K parts = 43 sec

      def _alterParts(chunk_size: Int): Unit = {
        val mqp = MetastoreQueryProcessorWithConnPool(spark, 12)
        val updatedParts = limitedPartitions.map(p =>
          p.copy(parameters = _updateData(data = p.parameters, updates = params))
        )
        println("Updating metastore partitions ...")
        ap(database, table, updatedParts, mqp)(_log, spark)
        println(s"Updated total ${updatedParts.length} parts")
      }

      timeit(
        () => _alterParts(chunk_size = 500),
        s"Updated ${limitedPartitions.length} parts in metastore, total time",
      )
      // pool(12) 20 sec
      // seq: chunk 500, 21 queries, 10K parts = 137 sec
      // par: chunk 500, 21 queries, 10K parts = 27 sec

    }
  }
}
