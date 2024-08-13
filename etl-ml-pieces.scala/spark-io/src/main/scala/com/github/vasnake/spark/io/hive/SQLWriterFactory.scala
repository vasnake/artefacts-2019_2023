/** Created by vasnake@gmail.com on 2024-07-23
  */
package com.github.vasnake.spark.io.hive

import com.github.vasnake.hive.SQLPartitionsWriterI
import org.apache.spark.sql.SparkSession

trait SQLWriterFactory {
  def getWriter(config: => WriterConfig): SQLPartitionsWriterI
}

trait WriterConfig {
  def spark: Option[SparkSession]
}
