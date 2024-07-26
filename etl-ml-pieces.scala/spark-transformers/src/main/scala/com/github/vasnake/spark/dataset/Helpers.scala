/**
 * Created by vasnake@gmail.com on 2024-07-26
 */
package com.github.vasnake.spark.dataset

import org.apache.spark.sql.DataFrame

object Helpers {

  def getNewTempColumnName(df: DataFrame): String = {
    val fnMaxLen = df.schema.fieldNames.map(_.length).max
    "u" * (fnMaxLen + 1)
  }

}
