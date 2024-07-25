/**
 * Created by vasnake@gmail.com on 2024-07-25
 */
package com.github.vasnake.spark.dataset.transform

/**
{{{
    max_target_rows_per_partition = int(float(max_target_rows) / config["shuffle_partitions"])
    if self.max_target_rows_per_partition > 0:
        self.info(
            "Selecting at most {} top-score rows ...".format(
                self.max_target_rows_per_partition * self.shuffle_partitions
            )
        )

        df = (
            df.repartition("uid")
            .withColumn(
                "_rn_",
                sqlfn.row_number().over(
                    Window.partitionBy(sqlfn.spark_partition_id()).orderBy(sqlfn.desc("score"))
                ),
            )
            .where("_rn_ <= {}".format(self.max_target_rows_per_partition))
            .drop("_rn_")
        )
}}}
 */
class TopNRowsApprox {
  def selectTopNRows() = ???
  def addLocalRank() = ???
}
