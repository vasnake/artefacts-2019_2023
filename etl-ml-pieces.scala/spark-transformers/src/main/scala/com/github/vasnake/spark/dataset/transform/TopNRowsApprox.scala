/** Created by vasnake@gmail.com on 2024-07-25
  */
package com.github.vasnake.spark.dataset.transform

import com.github.vasnake.spark.dataset.Helpers.getNewTempColumnName
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ functions => sf, _ }

object TopNRowsApprox {

  /** From each (sorted) partition select (n / partitions_count) top rows. No shuffle.
    * @param df input df should be evenly partitioned by some value independent from sort columns
    * @param n total number of selected rows, approximately
    * @param orderByColumns list of columns to define rows order
    * @param ascending sort direction
    * @return df with top n rows, approximately
    */
  def selectTopNRows(
    df: DataFrame,
    n: Long,
    orderByColumns: Seq[String],
    ascending: Boolean = false,
  ): DataFrame = {
    val rankCol: String = "_rank_" + getNewTempColumnName(df)

    val maxnPerPartition: Long =
      (n.toDouble / df.rdd.getNumPartitions.toDouble).toLong // well, df.rdd will cost you some

    val withRank = addLocalRank(df, orderByColumns, ascending, rankCol)

    withRank
      .where(s"$rankCol <= $maxnPerPartition")
      .drop(rankCol)
  }

  /** Local rank: for each partition compute isolated rank (1 .. num_rows_in_partition) in order of sorted rows. No shuffle.
    * @param df input df should be evenly partitioned by some value independent from sort columns
    * @param orderByColumns list of columns to define rows order
    * @param ascending sort direction
    * @param rankColName add column with this name, values are partition row numbers (1 .. partition_size) for each partition
    * @return DF sorted w/o shuffle, with a new column containing partitions row numbers
    */
  def addLocalRank(
    df: DataFrame,
    orderByColumns: Seq[String],
    ascending: Boolean,
    rankColName: String,
  ): DataFrame = {
    val orderColumns =
      if (ascending) orderByColumns map (sf.col(_).asc)
      else orderByColumns map (sf.col(_).desc)

    val w = Window
      .partitionBy(sf.spark_partition_id())
      .orderBy(orderColumns: _*)

    df.withColumn(
      rankColName,
      sf.row_number().over(w),
    )
  }
}
