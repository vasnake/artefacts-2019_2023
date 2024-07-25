/**
 * Created by vasnake@gmail.com on 2024-07-25
 */
package com.github.vasnake.spark.dataset.transform

/**
{{{
def select_top_rows(df, output_max_rows, order, *rank_columns):
    # > after orderBy operation not only each partition is sorted from within but
    # > also among each other, all_values(partition-0) < all_values(partition-1) < all_values(partition-2)
    # > a brilliant idea of leveraging this property of the DataFrame to assign a global rank to each row
    index_column = "_".join(df.schema.names)
    df = df.sort(*rank_columns, ascending=(order == "asc"))
    rdd = df.rdd.zipWithIndex()
    df = rdd.toDF().select(
        *([sqlfn.col("_1").getItem(c).alias(c) for c in df.schema.names] + [sqlfn.col("_2").alias(index_column)])
    )
    return df.where("{} < {}".format(index_column, output_max_rows)).drop(index_column)
}}}
 */
class TopNRowsExact {
  def selectTopNRows() = ???

  def addGlobalRankRDD() = ??? // zipWithIndex
  def addGlobalRankSQL() = ??? // window functions
}
