/**
 * Created by vasnake@gmail.com on 2024-07-23
 */
package com.github.vasnake.spark.io.hive

import org.apache.spark.sql.SparkSession
import com.github.vasnake.hive.SQLPartitionsWriterI

trait SQLWriterFactory {
  def getWriter(config: => WriterConfig): SQLPartitionsWriterI
}

trait WriterConfig {
  def spark: Option[SparkSession]
}
