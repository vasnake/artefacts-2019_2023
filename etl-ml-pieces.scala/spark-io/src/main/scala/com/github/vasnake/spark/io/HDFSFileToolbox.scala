/**
 * Created by vasnake@gmail.com on 2024-07-23
 */
package com.github.vasnake.spark.io

import org.apache.spark.sql.SparkSession

object HDFSFileToolbox {

  /**
    * Read text from `path` using sparkContext.textFile.
    * Obtain spark session using `SparkSession.builder().getOrCreate()`.
    *
    * @param path path to text file on hdfs (or local disk)
    * @return text combined from lines separated with '\n'
    */
  def loadTextFile(path: String): String = {
    val  spark: SparkSession = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    // sort lines by index in attempt to preserve lines order in file
    val lines = sc.textFile(path, minPartitions = 1)
      .zipWithIndex
      .collect
      .sortBy(_._2).map(_._1)

    lines.mkString("\n")
  }

}
