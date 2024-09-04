/**
 * Created by vasnake@gmail.com on 2024-08-21
 */
package com.github.vasnake.spark.io.hive

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{functions => sf}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql

object TableSmartReader {
  /**
    * This function increases data loading speed from tables (Hive, orc)
    * with a large number of files by parallel listing leaf files and directories.
    *
    * First, this function filters partitions from table by conditionExpression and
    * finds the paths of these partitions.
    * Second, it reads data from each path to DataFrame, adds partition columns with corresponding values and
    * adds columns to DataFrame with default value equal to null value of the given schema DataType
    * if DataFrame does not contain some columns from schema.
    * Finally, it unions these DataFrames.
    *
    * NOTE: conditionExpression must contain only partition columns!!!
    *
    * Example:
    * {{{
    * readTableAsUnionOrcFiles(
    *   "ods_data.access_log",
    *   col("dt").equalTo("2020-04-05") and col("source").isin("VFEED", "OFEED"),
    *   spark)
    *
    * will return the same result as
    *
    * spark.read.table("ods_data.access_log")
    *   .where(col("dt").equalTo("2020-04-05") and col("source").isin("VFEED", "OFEED"))
    *
    * }}}
    *
    * @param table database.table
    * @param conditionExpression expression for filtering partitions
    * @param spark SparkSession
    * @note conditionExpression must contain only partition columns!!!
    */
  def readTableAsUnionOrcFiles(
    table: String,
    conditionExpression: sql.Column,
    spark: sql.SparkSession
  ): sql.DataFrame = {

    val tmp_partition_values = "_tmp_partition_values_"
    val tmp_partition = "_tmp_partition_"

    val catalogTable = {
      val tableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(table)
      spark.sessionState.catalog.getTableMetadata(tableIdentifier)
    }

    val tableSchema = catalogTable.schema
    val location = catalogTable.location.toString
    val partitionColumnNamesWithIndex = catalogTable.partitionColumnNames.zipWithIndex.toMap

    // list all table partitions
    val partitionsDF = spark
      .sql(s"show partitions $table") // all table subdirectories as DF
      .withColumnRenamed("partition", tmp_partition)
      .withColumn(tmp_partition_values, sf.split(sf.col(tmp_partition), "/"))
      .withColumn(tmp_partition_values, sf.expr(s"TRANSFORM($tmp_partition_values, value -> split(value, '=')[1])"))

    val preparedPartitionsDF = partitionColumnNamesWithIndex.foldLeft(partitionsDF) {
      case (df, (name, index)) => df.withColumn(name, sf.col(tmp_partition_values)(index))
    }

    // we need only selected partitions (directories)
    val filteredPartitionDF = preparedPartitionsDF.where(conditionExpression)

    // collect partitions to array (path, values)
    val filteredPartitions: Array[(String, Array[String])] = filteredPartitionDF
      .select(tmp_partition, tmp_partition_values)
      .as[(String, Array[String])](Encoders.tuple(Encoders.STRING, ExpressionEncoder()))
      .collect()

    val pathsWithPartitionValues = filteredPartitions.map(p => (location + "/" + p._1 + "/*", p._2))

    if (pathsWithPartitionValues.length == 0)
      throw new org.apache.spark.SparkException(
        s"There is no partition in $table that satisfy condition '${conditionExpression.toString}'"
      )

    def readOneOrcPartition(path: String, partitionValues: Array[String]): sql.DataFrame = {
      val partDF = spark.read.orc(path) // only data columns, no partition data

      val partSchema = partDF.schema
      val maxIndex = partDF.columns.length - 1

      // restore table columns
      val allColumns = tableSchema.zipWithIndex.map {
        case (tableField, tableFieldIndex) =>

          if (partitionColumnNamesWithIndex.contains(tableField.name)) // it is a partition field
            sf.lit(partitionValues(partitionColumnNamesWithIndex(tableField.name))) // get value by index
              .cast(tableField.dataType)
              .as(tableField.name)

          else if (tableFieldIndex > maxIndex) // not a partition field, and (for some unknown reason) not in the DF
            sf.lit(null)
              .cast(tableField.dataType)
              .as(tableField.name)

          else if (!tableField.dataType.equals(partSchema(tableFieldIndex).dataType)) {
            // data field, and field type is messed-up?
            // it is a very stupid logic, it depends on the fact that table schema have data fields in front of partition fields
            sf.col(partSchema(tableFieldIndex).name)
              .cast(partSchema(tableFieldIndex).dataType)
              .as(tableField.name)
          }

          else if (tableField.name != partSchema(tableFieldIndex).name) // data field, and type ok, and name messed-up
            sf.col(partSchema(tableFieldIndex).name)
              .as(tableField.name)

          else sf.col(tableField.name) // data field, and type ok, and name ok
      }

      partDF.select(allColumns: _*)
    }

    // TODO: test 'par' version, should be faster a little
    val dataFrames = pathsWithPartitionValues.map {
      case (path, partitionValues) => readOneOrcPartition(path, partitionValues)
    }

    dataFrames.reduce((df1, df2) => df1.union(df2))
  }

}
