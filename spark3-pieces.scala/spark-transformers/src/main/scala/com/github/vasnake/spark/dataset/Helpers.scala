/** Created by vasnake@gmail.com on 2024-07-26
  */
package com.github.vasnake.spark.dataset

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

object Helpers { // TODO: rename to 'helpers'
  def getNewTempColumnName(ds: Dataset[_]): String =
    getNewTempColumnName(ds.schema)

  def getNewTempColumnName(schema: StructType): String = {
    val fnMaxLen = schema.fieldNames.map(_.length).max

    "u" * (fnMaxLen + 1)
  }
}
