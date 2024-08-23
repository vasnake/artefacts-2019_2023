package com.github.vasnake.spark.app.interview.transform_array

import org.apache.spark.sql._

/** Created by vasnake@gmail.com on 2024-08-07
  * {{{
  * Context: junior member of the team implemented spark-submit app.
  * Task for candidate: 'remember' what was the ticket in Jira about; perform code review; fix problems.
  * }}}
  */
object InvalidValuesToNullApp extends Serializable {
  @transient implicit lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val ds = spark
      .sql(
        s"select uid, features, dt from stage.user_features where dt='${args(0)}'"
      )
      .as[TableRow]

    val ds1 = ds.map { row =>
      row.copy(
        uid = row.uid,
        features =
          for (x <- row.features)
            yield
              if (x.isNaN || x.isInfinite) null.asInstanceOf[Float]
              else x,
        dt = row.dt
      )
    }

    ds1.write.mode(SaveMode.Overwrite).insertInto("stage.user_features")
  }

  case class TableRow(
    uid: String,
    features: Array[Float],
    dt: String
  )
}
