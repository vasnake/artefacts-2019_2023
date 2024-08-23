/** Created by vasnake@gmail.com on 2024-07-24
  */
package com.github.vasnake.`etl-core`.aggregate.config

/** Aggregation pipeline config
  * @param pipeline transformation steps names in order
  * @param stages definitions for each step
  */
case class AggregationPipelineConfig(
  pipeline: List[String],
  stages: Map[String, AggregationStageConfig]
) {
  override def toString: String =
    s"""AggregationPipelineConfig(
       |pipeline=List(${pipeline.mkString(",")}),
       |stages=${stages}
       |)""".stripMargin.trim
}

/** Definition of a transformation stage
  * @param name function name, e.g. `min`, `avg`, `sum`
  * @param kind stage type: `filter`, `agg`, `imputer`, etc
  * @param parameters function parameters, see function spec.
  */
case class AggregationStageConfig(
  name: String,
  kind: String,
  parameters: Map[String, String]
) {
  override def toString: String =
    s"""AggregationStageConfig(
       |name=${name},
       |type=${kind},
       |parameters=${parameters}
       |)""".stripMargin.trim
}
