package com.github.vasnake.spark.io.stats

/**
 * Created by vasnake@gmail.com on 2024-07-23
 */

import com.github.vasnake.common.num.{FastMath => fm}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{functions => sqlfn}

import scala.collection.immutable.HashMap

/**
  * Partitions (Hive) information gathered from DataFrame.
  * @param partitionColumnsNames ordered partition columns names.
  * @param partitionColumnsIndices ordered partition columns indices in dataframe.
  * @param dataFramePartitions partitions discriminators, each partition is a set of column names with associated value for each name (col_name: part_value).
  * @param dataFramePartitionsStats counts for each partition, partition key is an encoded set of values from dataFramePartitions, e.g. `2021-12-02/42`
  */
case class DataFrameBucketsStats(
                                        partitionColumnsNames: Seq[String] = Seq.empty,
                                        partitionColumnsIndices: Seq[Int] = Seq.empty,
                                        dataFramePartitions: Seq[HashMap[String, String]] = Seq.empty,
                                        dataFramePartitionsStats: HashMap[String, DataFrameBuckets] = HashMap.empty
                                      ) {
  // N.B. HashMap serializable, Map is not (not always, anyway)

  override def toString: String = {
    import DataFrameBucketsStats.map2str

    s"""DataFrameBucketsStats(
        |  partitionColumnsNames=Seq(${partitionColumnsNames.mkString("\"", "\", \"", "\"")}),
        |  partitionColumnsIndices=Seq(${partitionColumnsIndices.mkString("\"", "\", \"", "\"")}),
        |  dataFramePartitions=Seq(${dataFramePartitions.map(map2str).mkString(",\n")}),
        |  dataFramePartitionsStats=${map2str(dataFramePartitionsStats)}
        |)
      |""".stripMargin.trim
  }

  /**
    * Check if given partition is in collected partitions info.
    * @param partition set of column names with associated value for each name.
    * @return true if partition is in dataFramePartitions, false otherwise.
    */
  def contains(partition: Map[String, String]): Boolean = {
    dataFramePartitionsStats.contains(
      DataFrameBucketsStats.buildPartitionID(partition, partitionColumnsNames)
    )
  }

  def setupBuckets(maxRowsPerBucket: Int): DataFrameBucketsStats = {
    var totalBuckets: Int = 0 // TODO: use foldLeft

    val stats = dataFramePartitionsStats.map { case (partKey, counts) => {
      val updatedItem = partKey -> DataFrameBuckets(
        rowsCount = counts.rowsCount,
        bucketsCount = fm.ceil(counts.rowsCount.toDouble / maxRowsPerBucket.toDouble).toInt,
        firstBucket = totalBuckets
      )

      totalBuckets += updatedItem._2.bucketsCount

      updatedItem
    }}

    this.copy(dataFramePartitionsStats = stats)
  }

  def buildPartitionID(row: Row): String =
    DataFrameBucketsStats.buildPartitionID(
      partitionColumnsIndices.map(colIdx => row.getString(colIdx))
    )

}

object DataFrameBucketsStats {

  def apply(
             partitionColumns: Seq[String],
             df: DataFrame,
             maxRowsPerBucket: Int
           ): DataFrameBucketsStats = {

    val countColumnName: String = s"""${"a" * df.schema.fieldNames.map(_.length).max}_count"""

    val countsDF: DataFrame = df.groupBy(partitionColumns.map(
      name => sqlfn.col(name)
    ) :_* )
      .agg(sqlfn.count(sqlfn.lit(1)).cast(DataTypes.StringType) alias countColumnName) // unify result data types to string

    val partitionsWithRowsCount: Seq[Map[String, String]] = countsDF.collect().map(row =>
      row.getValuesMap[String](partitionColumns :+ countColumnName) // N.B. row.getValuesMap[String] doesn't really do conversion to String
    )

    require(
      partitionsWithRowsCount.forall(_.values.forall(Option(_).isDefined)),
      s"NULL in partition columns is not allowed, got: ${partitionsWithRowsCount.mkString("; ")}"
    )

    if (partitionsWithRowsCount.isEmpty) new DataFrameBucketsStats() // empty stats from empty dataframe
    else new DataFrameBucketsStats(
      partitionColumnsNames = partitionColumns,
      partitionColumnsIndices = partitionColumns.map(name => df.schema.fieldIndex(name)),

      // map colname: value for each partition
      dataFramePartitions = partitionsWithRowsCount.map(row =>
        row.filterKeys(_ != countColumnName).toHashMap
      ),

      // map part_key: counts for each partition
      dataFramePartitionsStats = partitionsWithRowsCount.map(row =>
        buildPartitionID(row, partitionColumns) -> DataFrameBuckets(rowsCount = row(countColumnName).toLong)
      ).toMap.toHashMap

    ).setupBuckets(maxRowsPerBucket)
  }

  def buildPartitionID(orderedValues: Seq[String]): String =
    orderedValues.mkString("/")

  def buildPartitionID(partition: Map[String, String], columns: Seq[String]): String =
    buildPartitionID(columns map partition)

  def summaryMessage(stats: DataFrameBucketsStats): String = {
    // r rows in f files in p partitions; keys: a audience_name, b category, c dt, d uid_type
    val keysCountStr = {
      val counts: Map[String, Int] = stats.dataFramePartitions.flatMap(_.toSeq) // Seq[(String, String)]
        .groupBy { case (k, _) => k } // Map[String, Seq[(String, String)]
        .map { case (k, v) => k -> v.map(_._2).toSet.size } // partition key -> count of partition values

      stats.partitionColumnsNames.map(n => s"${counts(n)} $n").mkString(", ")
    }

    val rowsCount = stats.dataFramePartitionsStats.values.map(_.rowsCount).sum
    val filesCount = stats.dataFramePartitionsStats.values.map(_.bucketsCount).sum

    s"$rowsCount rows in $filesCount files in ${stats.dataFramePartitions.length} dirs; keys: $keysCountStr"
  }

  def map2str[A, B](m: Map[A, B]): String = {
    s"Map(${m.toSeq.map(tup2str).mkString(",\n")})"
  }

  def tup2str[A, B](t: (A, B)): String = {
    s"(${t._1} -> ${t._2})"
  }

  implicit class MapOps[A, B](val m: Map[A, B]) {
    def toHashMap: HashMap[A, B] = HashMap(m.toSeq : _*)
  }

}

case class DataFrameBuckets(
                             rowsCount: Long = -1L, // min valid value = 1
                             bucketsCount: Int = -1, // min valid value = 1
                             firstBucket: Int = -1 // min valid value = 0
                           ) {
  override def toString: String =
    s"DataFrameBuckets(rowsCount=${rowsCount}, bucketsCount=${bucketsCount}, firstBucket=${firstBucket})"
}
