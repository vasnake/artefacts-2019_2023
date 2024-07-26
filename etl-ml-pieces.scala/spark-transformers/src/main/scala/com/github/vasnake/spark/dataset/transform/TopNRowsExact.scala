/**
 * Created by vasnake@gmail.com on 2024-07-25
 */
package com.github.vasnake.spark.dataset.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => sf}
import com.github.vasnake.spark.dataset.Helpers.getNewTempColumnName
import org.apache.spark.sql.expressions.Window

object TopNRowsExact {

  private def debug(text: String): Unit = { // TODO: add proper logging
    println(text)
  }

  /**
   * Add global rank (see `rankFun`) and select rows where `rank <= n`
   * @param df input DF
   * @param n maximum rank value
   * @param orderByColumns list of columns to define rows order
   * @param ascending sort direction
   * @param rankFun rank function implementation, `globalRankRDD` and `globalRankSQL` are available
   * @return `n` or less top rows from sorted `df`
   */
  def selectTopNRows(df: DataFrame, n: Long, orderByColumns: Seq[String], ascending: Boolean = false, rankFun: GlobalRank = globalRankRDD): DataFrame = {
    val rankCol: String = "_rank_" + getNewTempColumnName(df)
    val withRank = rankFun.add(df, orderByColumns, ascending, rankCol)

    val condition = if (rankFun.isZeroBasedRank) "<" else "<="

    withRank
      .where(s"$rankCol $condition $n")
      .drop(rankCol)
  }

  // zipWithIndex
  val globalRankRDD: GlobalRank = new GlobalRank {
    val isZeroBasedRank: Boolean = true

    override def add(df: DataFrame, orderByColumns: Seq[String], ascending: Boolean, rankColName: String): DataFrame = {
      import df.sparkSession.implicits._

      // sort
      val orderColumns =
        if (ascending) orderByColumns map (sf.col(_).asc)
        else orderByColumns map (sf.col(_).desc)

      val orderedDF = df.orderBy(orderColumns :_*)

      // add rank
      val rddOfPairs = orderedDF.rdd.zipWithIndex() // tuple (row, index) // index (0 .. count - 1)

      // decode from RDD
      val origColumns = df.schema.fieldNames.map {
        name => sf.col("_1").getItem(name).alias(name)
      }
      val origColumnsAndRank = origColumns :+ sf.col("_2").alias(rankColName)

      df.sparkSession
        .createDataset(rddOfPairs)
        .select(origColumnsAndRank :_*)
    }
  }

  // window functions
  val globalRankSQL: GlobalRank = new GlobalRank {
    val isZeroBasedRank: Boolean = false

    override def add(df: DataFrame, orderByColumns: Seq[String], ascending: Boolean, rankColName: String): DataFrame = {
      // sort
      val orderColumns =
        if (ascending) orderByColumns map (sf.col(_).asc)
        else orderByColumns map (sf.col(_).desc)

      val orderedDF = df.orderBy(orderColumns: _*)

      // add partition_id, should be in total order
      val partIdColumn = "partition_id_" + rankColName
      val withPartitionIdDF = orderedDF.withColumn(partIdColumn, sf.spark_partition_id())

      // rank within each partition (row number for partition?)
      val localRankColumn = "local_rank_" + rankColName
      val partitionW = Window.partitionBy(partIdColumn).orderBy(orderColumns :_*)
      val withLocalRankDF = withPartitionIdDF.withColumn(localRankColumn, sf.rank().over(partitionW)) // rank or row_number?

      // (groupBy partition_id), tiny statsDF: max rank and cum-sum for max rank (partition rows count)
      val cumSumMaxRankColumn = "cum_sum_max_rank_" + rankColName
      val statsDF = {
        val maxLocalRankColumn = "max_local_rank_" + rankColName
        val maxLocalRankDF = withLocalRankDF.groupBy(partIdColumn).agg(sf.max(localRankColumn).alias(maxLocalRankColumn))

        // DF has rows.count = partitions.count, no need to do `partitionBy` in window definition
        val prevRowsW = Window.orderBy(partIdColumn).rowsBetween(Window.unboundedPreceding, Window.currentRow)

        // rows count in partition and all previous partitions
        val cachedStatsDF = maxLocalRankDF.withColumn(cumSumMaxRankColumn, sf.sum(maxLocalRankColumn).over(prevRowsW))
          .cache() // TODO: check if it helps, probably not
        debug(s"GlobalRank, force cache materialization, stats DF rows count: ${cachedStatsDF.count()}")
        cachedStatsDF
      }

      // Calc 'sum_factor' (number of rows) that have to be added to local rank (local row number) in each partition.
      // For first partition 'sum_factor" has to be 0, for second = first-partition-rows.count, ...
      // So, we have to shift statsDF rows and save partition_id
      val sumFactorColumn = "sum_factor_" + rankColName
      val shiftedStatsDF = statsDF.alias("l").join(statsDF.alias("r"),
        sf.col(s"l.$partIdColumn") === sf.col(s"r.$partIdColumn") + 1,
        "left"
      ).select(
        sf.col(s"l.$partIdColumn").alias(partIdColumn),
        sf.coalesce(sf.col(s"r.$cumSumMaxRankColumn"), sf.lit(0)).alias(sumFactorColumn)
      )

      // total_rank = local_rank + sum_factor
      val withTotalRankDF = withLocalRankDF.join(sf.broadcast(shiftedStatsDF), Seq(partIdColumn), "inner")
        .withColumn(rankColName, sf.col(localRankColumn) + sf.col(sumFactorColumn))

      // drop intermediate columns: part_id, local_rank, sum_factor
      withTotalRankDF.drop(partIdColumn, localRankColumn, sumFactorColumn)
    }

  }

  /**
<pre>
> after orderBy operation not only each partition is sorted from within but
> also among each other, all_values(partition-0) < all_values(partition-1) < all_values(partition-2)
> a brilliant idea of leveraging this property of the DataFrame to assign a global rank to each row
# First implemented in Python
def select_top_rows(df, output_max_rows, order, *rank_columns):
    index_column = "_".join(df.schema.names)
    df = df.sort(*rank_columns, ascending=(order == "asc"))
    rdd = df.rdd.zipWithIndex()
    df = rdd.toDF().select(
        ([sqlfn.col("_1").getItem(c).alias(c) for c in df.schema.names] + [sqlfn.col("_2").alias(index_column)])
    )
    return df.where("{} < {}".format(index_column, output_max_rows)).drop(index_column)
</pre>
   */
  trait GlobalRank {
    def add(df: DataFrame, orderByColumns: Seq[String], ascending: Boolean, rankColName: String): DataFrame
    def isZeroBasedRank: Boolean
  }

}
