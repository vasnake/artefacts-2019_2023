/** Created by vasnake@gmail.com on 2024-07-10
  */
package com.github.vasnake.core.aggregation

import scala.annotation.tailrec

import com.github.vasnake.core.aggregation.config._

/** list of stages
  */
trait AggregationPipelineConfigured extends Aggregator[Double] {
  def copy(): AggregationPipelineConfigured
}

trait Aggregator[T] {
  def start(vectorLength: Int): Unit
  def add(item: T): Unit
  def result: T
}

// pipelines implementation

case class TransformersPipeline(stages: Seq[AggregationStage])
    extends AggregationPipelineConfigured {
  // TODO: possible optimization: process items w/o acc buffer
  private var acc: Array[Double] = _
  private var idx: Int = 0
  // mutable state should not be shared!
  override def copy(): AggregationPipelineConfigured = TransformersPipeline(stages)

  def start(vectorLength: Int): Unit = {
    acc = new Array[Double](vectorLength)
    idx = 0
  }

  def add(item: Double): Unit = {
    acc(idx) = item
    idx += 1
  }

  def result: Double = applyStages(stages, acc).head // nan or value

  @tailrec
  private def applyStages(stages: Seq[AggregationStage], data: Array[Double]): Array[Double] =
    stages match {
      case Nil => data
      case h :: t => applyStages(t, h.transform(data))
    }
}

object AggregationPipelineConfigured {
  // build pipeline from config
  def apply(cfg: AggregationConfig): AggregationPipelineConfigured = {
    val stages: Seq[AggregationStage] = cfg.pipeline.map { stageName =>
      val stageCfg: AggregationStageConfig = cfg
        .stages
        .getOrElse(stageName, sys.error(s"Can't find stage `${stageName}` in config `${cfg}`"))
      stage(stageCfg)
    }

    TransformersPipeline(stages)
  }

  def stage(cfg: AggregationStageConfig): AggregationStage =
    cfg.kind match {
      case "filter" => filterStage(cfg.name, cfg.parameters)
      case "agg" => aggStage(cfg.name, cfg.parameters)
      case _ => sys.error(s"Unknown stage type: `${cfg.kind}` in cfg: `${cfg}`")
    }

  def filterStage(name: String, params: Map[String, String]): AggregationStage =
    name match {
//      case "drop_null" => FilterDropNull()
      case "drop_le_threshold" => ??? // TODO: threshold value, compare le|ge
      case "drop_ge_threshold" => ???
      case _ => sys.error(s"Unknown filter function name `${name}`")
    }

  def aggStage(name: String, params: Map[String, String]): AggregationStage =
    name match {
      case "avg" => AggAverage(params)
      case "min" => AggMin()
      case "max" => AggMax()
      case "sum" => AggSum()
      case "most_freq" => AggMostFreq(params)
      case _ => sys.error(s"Unknown aggregation function name `${name}`")
    }
}
