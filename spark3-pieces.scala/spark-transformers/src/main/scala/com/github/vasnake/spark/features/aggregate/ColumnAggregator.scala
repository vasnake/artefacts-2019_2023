/** Created by vasnake@gmail.com on 2024-07-24
  */
package com.github.vasnake.spark.features.aggregate

import scala.collection.mutable

import com.github.vasnake.`etl-core`.aggregate._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait ColumnAggregator {
  def apply(rows: Seq[Row]): Any
}

// implementations

case class UidFieldAggregator(colIdx: Int) extends ColumnAggregator {
  def apply(rows: Seq[Row]): Any = rows.head.get(colIdx)
}

case class ScalarFieldAggregator(
  colIdx: Int,
  colStruct: StructField,
  pipelines: SetOfNamedPipelines
) extends ColumnAggregator {
  def apply(rows: Seq[Row]): Any = {
    val agg = pipelines.features.getOrElse(colStruct.name, pipelines.default).copy()
    agg.start(rows.length)
    rows.foreach(row => agg.add(fieldValue(row)))
    result(agg.result)
  }

  private def fieldValue(row: Row): Double = {
    val v = row.getAs[Any](colIdx)
    if (v == null) Double.NaN
    else v.asInstanceOf[Float].toDouble
  }

  private def result(value: Double): Any =
    if (value.isNaN) null
    else value.toFloat
}

case class ArrayFieldAggregator(
  colIdx: Int,
  colStruct: StructField,
  aggPipelines: SetOfNamedPipelines
) extends ColumnAggregator {
  def apply(rows: Seq[Row]): Any = {
    // apply pipeline for each feature, return array
    // TODO: speed optimization needed

    val pipelines = aggPipelines.copy()

    val result: Option[mutable.WrappedArray[Any]] = rows
      .find(row => !row.isNullAt(colIdx))
      .map(row => row.getAs[mutable.WrappedArray[Any]](colIdx).clone())

    val numRows = rows.length

    result.foreach(res =>
      res.indices.foreach { idx =>
        val agg: AggregationPipeline = pipelines.features.getOrElse(s"${idx}", pipelines.default)
        agg.start(numRows)
        rows.foreach(row => agg.add(arrayValue(row, idx)))
        val v = agg.result

        if (v.isNaN) res(idx) = null
        else res(idx) = v.toFloat
      }
    )

    result.orNull
  }

  private def arrayValue(row: Row, idx: Int): Double =
    if (row.isNullAt(colIdx)) Double.NaN
    else {
      val arr = row.getAs[mutable.WrappedArray[Any]](colIdx)
      if (arr(idx) == null) Double.NaN
      else arr(idx).asInstanceOf[Float].toDouble
    }
}

case class MapFieldAggregator(
  colIdx: Int,
  colStruct: StructField,
  aggPipelines: SetOfNamedPipelines
) extends ColumnAggregator {
  def apply(rows: Seq[Row]): Any = {
    // apply pipeline for each feature, return map
    // TODO: speed optimization needed

    val pipelines = aggPipelines.copy()

    val result = mutable.Map.empty[String, Any]
    val keys: Seq[String] = collectKeys(rows)
    val numRows = rows.length

    keys.foreach { key =>
      val agg: AggregationPipeline = pipelines.features.getOrElse(key, pipelines.default)
      agg.start(numRows)
      rows.foreach(row => agg.add(mapValue(row, key)))
      val v = agg.result

      if (!v.isNaN) result(key) = v.toFloat
    }

    if (result.isEmpty) null
    else result
  }

  private def mapValue(row: Row, key: String): Double =
    if (row.isNullAt(colIdx)) Double.NaN
    else {
      val m = row.getAs[Map[String, Any]](colIdx)
      val v = m.getOrElse(key, null)

      if (v == null) Double.NaN
      else v.asInstanceOf[Float].toDouble
    }

  private def collectKeys(rows: Seq[Row]): Seq[String] =
    rows.flatMap { row =>
      val m =
        if (row.isNullAt(colIdx)) Map.empty[String, Any]
        else row.getAs[Map[String, Any]](colIdx)

      m.keys
    }.distinct
}

case class SetOfNamedPipelines(
  default: AggregationPipeline,
  features: Map[String, AggregationPipeline]
) {
  def copy(): SetOfNamedPipelines =
    SetOfNamedPipelines(default.copy(), features.map(kv => kv._1 -> kv._2.copy()))
}
// TODO: remove hardcoded cast to Float
// TODO: DRY
