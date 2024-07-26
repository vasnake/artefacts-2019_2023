/**
 * Created by vasnake@gmail.com on 2024-07-25
 */
package com.github.vasnake.spark.dataset.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => sf}

import com.github.vasnake.spark.dataset.Helpers.getNewTempColumnName

object TopNRowsExact {

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
      val rdd = orderedDF.rdd.zipWithIndex() // (row, index) // index (0 .. count - 1)

      // decode from RDD
      val origColumns = df.schema.fieldNames.map {
        name => sf.col("_1").getItem(name).alias(name)
      }
      val columnsWithRank = origColumns :+ sf.col("_2").alias(rankColName)

      df.sparkSession
        .createDataset(rdd)
        .select(columnsWithRank :_*)
    }
  }

  val globalRankSQL: GlobalRank = new GlobalRank {
    // window functions
    override def add(df: DataFrame, orderByColumns: Seq[String], ascending: Boolean, rankColName: String): DataFrame = ???
    val isZeroBasedRank: Boolean = ???
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
