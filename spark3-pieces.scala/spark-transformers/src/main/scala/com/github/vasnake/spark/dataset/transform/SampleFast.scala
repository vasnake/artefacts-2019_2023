/**
 * Created by vasnake@gmail.com on 2024-09-19
 */
package com.github.vasnake.spark.dataset.transform

import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.annotation.tailrec

// First X rows will be taken from first Y partitions of input dataset.
object SampleFast {

  def apply[T](ds: Dataset[T], sampleSize: Int): Dataset[T] = {
    ds.sparkSession.createDataset(
      apply(ds.rdd, sampleSize)
    )(ds.encoder)
  }

  def sampleDF(df: DataFrame, sampleSize: Int): DataFrame = {
    df.sparkSession.createDataFrame(
      apply(df.rdd, sampleSize),
      df.schema
    )
  }

  def apply[T](rdd: RDD[T], sampleSize: Int): RDD[T] = {
    // start with first partition
    val numParts = findFirstNPartitions(rdd, sampleSize, partsIndices = Array(0))

    rdd.coalesce(
      numPartitions = 1, // should be N
      shuffle = false,
      partitionCoalescer = Option(new FirstNCoalescer(numParts))
    )
  }

  @tailrec
  def findFirstNPartitions[_](rdd: RDD[_], needRows: Int, partsIndices: Array[Int]): Int = {
    val rowsInPartition: Iterator[_] => Int =
      it => it.take(needRows).size // materialize min(needRows, it.size) rows

    val partsSize: Array[Int] = rdd.sparkContext.runJob(rdd, rowsInPartition, partsIndices)

    if (partsSize.sum >= needRows) {
      // enough rows
      val (_, idx) = partsSize.zip(partsIndices)
        .foldLeft((needRows, partsIndices.head)) {
          case ((rowsCount, currIdx), (pSize, pIdx)) => {
            if (rowsCount <= 0) (rowsCount, currIdx)
            else (rowsCount - pSize, pIdx)
          }
        }
      idx + 1 // convert index to count (we need partitions with indices 0 .. idx)
    } else {
      // not enough rows, need more partitions
      val ratio = needRows / partsSize.sum // 11/6=1; 12/6=2 // tune-up number of partitions to check
      val firstIdx = partsIndices.last + 1
      val lastIdx = firstIdx + (partsSize.length * ratio)
      val newPartsIndices: Seq[Int] = firstIdx until lastIdx
      val newNeedRows = needRows - partsSize.sum
      findFirstNPartitions(rdd, newNeedRows, newPartsIndices.toArray)
    }
  }

  class FirstNCoalescer(val firstNPartitions: Int) extends PartitionCoalescer with Serializable {
    override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
      // one group of N partitions, should be N groups
      val group: PartitionGroup = new PartitionGroup()
      group.partitions ++= parent.partitions.slice(0, firstNPartitions)

      // one part. out, should be N parts
      Array(group)
    }
  }

}

// Original code, saved for teaching purposes.
// First X rows will be taken from first Y partitions of input dataset.
object SampleFast_V1 {
  // Got from https://gist.github.com/xhumanoid/51d3cb21675ff035fe057d0c0ae29dce
  // which was inspired by code from 'RDD.take(n)'

  def approxLazyLimit(dataFrame: DataFrame, limit: Int): DataFrame = {
    val rdd = dataFrame.rdd
    val spark = dataFrame.sparkSession
    val sc = spark.sparkContext
    val totalParts = rdd.partitions.length

    var left = limit
    var partsScanned = 0
    while (left > 0 && partsScanned < totalParts) {
      // start current iteration with 1 partition to scan
      var numPartsToTry = 1L
      // if we already scan some partitions and don't have enough of data yet: scale up
      // scale up by 2 * already_scanned_partitions
      if (partsScanned > 0) {
        numPartsToTry = partsScanned * 2
      }

      // scan subset of partitions only
      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      // list of parts indices

      // part idx => rows.count
      val partLimitSize: Array[Int] = sc.runJob(
        rdd,
        (it: Iterator[Row]) => it.take(left).size, // materialize min(left, it.size) rows
        p
      )

      // update rows count and parts. count
      partLimitSize.foreach(c =>
        if (left > 0) {
          left -= c
          partsScanned += 1
        }
      )
    } // end while

    // return new dataframe with limited minimum partitions set
    spark.createDataFrame(
      rdd.coalesce(
        numPartitions = 1,
        shuffle = false,
        partitionCoalescer = Option(new CustomCoalesce(partsScanned))
      ),
      dataFrame.schema
    )
  }

  class CustomCoalesce(val size: Int) extends PartitionCoalescer with Serializable {
    // 'size' part. in

    override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
      val group: PartitionGroup = new PartitionGroup()
      group.partitions ++= parent.partitions.slice(0, size)

      // one part. out
      Array(group)
    }
  }

  // Sample size expected to be not larger than num of rows in first couple of partitions.
  def test(dataFrame: DataFrame, sampleRecords: Int = 10000, samplingDir: String): Unit = {
    // cached dataframe on multiple terabytes
    // val dataFrame: DataFrame = ???

    // sampleRecords = 10_000 by default
    // val sampleRecords = 10000

    // storage path
    // val samplingDir: String = ???

    // slow
    // val sampleDF = dataFrame.limit(sampleRecords)
    // sampleDF.repartition(1).write.parquet(samplingDir)

    // fast
    val sampleDF = approxLazyLimit(dataFrame, sampleRecords).limit(sampleRecords)
    sampleDF.repartition(1).write.parquet(samplingDir)
  }

}
