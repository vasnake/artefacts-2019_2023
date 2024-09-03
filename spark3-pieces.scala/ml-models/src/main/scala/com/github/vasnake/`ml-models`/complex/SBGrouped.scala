/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`ml-core`.models.interface._
// TODO: move to ml-core

// SB: Sample Based, sample groups used to train model
case class SBGroupedTransformerConfig(groups: Map[String, PostprocessorConfig])

case class SBGroupedTransformer // TODO: rename to GroupBasedTransformer
(
  config: SBGroupedTransformerConfig,
  group: String,
  transformerFactory: PostprocessorConfig => InplaceTransformer
) extends InplaceTransformer {
  require(
    group.isEmpty || config.groups.contains(group),
    s"can't find transformer '${group}'"
  )

  private val transformer: Option[InplaceTransformer] = {
    val trf = if (group.isEmpty) None else Some(transformerFactory(config.groups(group)))
//    val log = LoggerFactory.getLogger(getClass)
//    log.info(s"for group '${group}' found transformer '${trf}' ")

    trf
  }

  def transform(vec: Array[Double]): Unit = {
    require(vec.nonEmpty, "input must not be empty")
    if (transformer.isDefined) transformer.get.transform(vec)
    // should be faster than
    // transformer.foreach(_.transform(vec))
  }
}
