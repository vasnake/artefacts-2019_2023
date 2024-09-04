/** Created by vasnake@gmail.com on 2024-07-24
  */
package com.github.vasnake.spark.features.aggregate

import com.github.vasnake.`etl-core`.aggregate.AggregationPipeline
import com.github.vasnake.`etl-core`.aggregate.config.AggregationPipelineConfig

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql
import sql._

/** Single feature represented by a number, float or double.
  * Group of features (dense vector as array, sparse vector as map) represented by 3 types of columns:
  * Map, Array, and set of primitive columns with common prefix where prefix used as group (or 'domain') name.
  */
object DatasetAggregator { // TODO: move to com.github.vasnake.spark.dataset.transform.ConfiguredAggregate
  type ColumnsAggregationConfig = Map[String, AggregationPipelineConfig]
  val GROUPBY_COL_NAME: String = "uid"
  val DEFAULT_PIPELINE_KEY: String = "domain"

  case class DatasetAggregators(columnsAgg: Seq[ColumnAggregator])

  def aggregateColumns(
    df: DataFrame,
    cfg: Map[String, ColumnsAggregationConfig]
  )(implicit
    spark: SparkSession
  ): DataFrame = {

    import spark.implicits._
    // import sql.catalyst.encoders.RowEncoder
    // implicit val outRowEncoder: Encoder[Row] = RowEncoder.encoderFor(df.schema)
    implicit val outRowEncoder: Encoder[Row] = sql.Encoders.row(df.schema)

    val aggregators: Broadcast[DatasetAggregators] = spark
      .sparkContext
      .broadcast(
        compileAggregators(cfg, df)
      )

    df.groupByKey(row => row.getAs[String](GROUPBY_COL_NAME))
      .mapGroups { case (key, rows) => reduceRows(key, rows, aggregators) }
  }

  def reduceRows(
    key: String,
    iter: Iterator[Row],
    aggregators: Broadcast[DatasetAggregators]
  ): Row = {
    val aggs = aggregators.value
    val rows: Seq[Row] = iter.toSeq

    val aggregatedColumns = for {
      aggregator <- aggs.columnsAgg
    } yield aggregator.apply(rows)

    Row.fromSeq(aggregatedColumns)
  }

  def compileAggregators(cfg: Map[String, ColumnsAggregationConfig], df: DataFrame)
    : DatasetAggregators = {
    // analyze schema and build aggregators on driver
    import sql.types._

    def findGroupName(prefixedFieldName: String): String = cfg
      .keys
      .find(groupName => prefixedFieldName.startsWith(s"${groupName}_"))
      .getOrElse(prefixedFieldName)

    // TODO: KISS, DRY

    def aggPipeline(fieldName: String): AggregationPipeline = {
      val cAggCfg: ColumnsAggregationConfig = cfg.getOrElse(
        fieldName,
        sys.error(s"Can't find `${fieldName}` config in `${cfg}`")
      )
      val apCfg: AggregationPipelineConfig = cAggCfg.getOrElse(
        fieldName,
        cAggCfg.getOrElse(
          DEFAULT_PIPELINE_KEY,
          sys.error(s"Can't find default config in `${cAggCfg}`")
        )
      )

      AggregationPipeline(apCfg)
    }

    def buildScalarFieldAggPipeline(fieldName: String): SetOfNamedPipelines = {
      val groupName: String = findGroupName(fieldName)
      val defaultPipeline: AggregationPipeline = aggPipeline(groupName)

      val featuresPipelines: Map[String, AggregationPipeline] = {
        val cAggCfg: ColumnsAggregationConfig = cfg.getOrElse(
          groupName,
          sys.error(s"Can't find `${groupName}` config in `${cfg}`")
        )
        cAggCfg
          .keys
          .filter(k => k != DEFAULT_PIPELINE_KEY)
          .map(featureName => (featureName, AggregationPipeline(cAggCfg(featureName))))
          .toMap
      }

      SetOfNamedPipelines(featuresPipelines.getOrElse(fieldName, defaultPipeline), Map.empty)
    }

    def buildCollectionFieldAggPipeline(fieldName: String): SetOfNamedPipelines = {
      val defaultPipeline: AggregationPipeline = aggPipeline(fieldName)

      val featuresPipelines: Map[String, AggregationPipeline] = {
        val cAggCfg: ColumnsAggregationConfig = cfg.getOrElse(
          fieldName,
          sys.error(s"Can't find `${fieldName}` config in `${cfg}`")
        )
        cAggCfg
          .keys
          .filter(k => k != DEFAULT_PIPELINE_KEY)
          .map(featureName => (featureName, AggregationPipeline(cAggCfg(featureName))))
          .toMap
      }

      SetOfNamedPipelines(defaultPipeline, featuresPipelines)
    }

    val aggregators: Seq[ColumnAggregator] = df.schema.map { field =>
      val idx = df.schema.fieldIndex(field.name)

      field.dataType match {
        case StringType if field.name == GROUPBY_COL_NAME => UidFieldAggregator(idx)
        case MapType(_, _, _) =>
          MapFieldAggregator(idx, field, buildCollectionFieldAggPipeline(field.name))
        case ArrayType(_, _) =>
          ArrayFieldAggregator(idx, field, buildCollectionFieldAggPipeline(field.name))
        case _ => ScalarFieldAggregator(idx, field, buildScalarFieldAggPipeline(field.name))
      }
    }

    DatasetAggregators(aggregators)
  }
}
