/**
 * Created by vasnake@gmail.com on 2024-07-23
 */
package com.github.vasnake.spark.io

import org.apache.spark.sql.DataFrame

trait CheckpointService {
  def checkpoint(df: DataFrame): DataFrame // TODO: consider signature DF => F[DF]
}
