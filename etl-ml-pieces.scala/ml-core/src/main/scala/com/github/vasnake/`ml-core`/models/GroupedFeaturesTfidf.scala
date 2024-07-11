/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-core`.models

import com.github.vasnake.`ml-core`.models.interface.GroupedFeaturesTransformer
import com.github.vasnake.common.num.FastMath

// TODO: rename GroupedFeaturesTfidfTransformer to GroupedFeaturesTfidf
case class GroupedFeaturesTfidfTransformer(config: GroupedFeaturesTfidfTransformerConfig)
    extends GroupedFeaturesTransformer {
  private val sublinear_tf = config.transformer_params("sublinear_tf").toBoolean // default false
  private val use_idf = config.transformer_params("use_idf").toBoolean // default true

  require(
    if (use_idf) {
      val gs = config.groups
      val ds = config.idf_diags
      ds.length == gs.length && gs.zip(ds).forall(gd => gd._1.length == gd._2.length)
    }
    else true,
    "Shape for groups and idf_diags must be the same",
  )

  import GroupedFeaturesTfidfTransformerConfig._

  private val norm = config.transformer_params("norm") match { // default "l1"
    case "l1" => NORM_L1
    case "l2" => NORM_L2
    case x =>
//      warn(s"unknown `norm` parameter: '${x}'")
      NO_NORM
  }

  def transform(input: Array[Double]): Unit = {
    require(
      input.length == config.n_features,
      s"Transforming vector size must be the same size as training sample vectors: '${config.n_features}'",
    )

    if (config.groups.nonEmpty) {

      // loop over all groups
      var groupNum: Int = 0
      var idf_diag: Array[Double] = null
      var numInGroup: Int = 0
      var normVal: Double = 0.0
      var _normVal: Double = 0.0

      config.groups.foreach { groupIndices =>
        idf_diag = config.idf_diags(groupNum)

        // idf loop over current group
        numInGroup = 0
        groupIndices.foreach { featureIndex =>
          if (sublinear_tf)
            input(featureIndex) = FastMath.log(input(featureIndex) + 1)
          if (use_idf) {
            input(featureIndex) = input(featureIndex) * idf_diag(numInGroup)
            numInGroup += 1
          }
        }

        // norm loop over current group, two loops in fact
        norm match {
          case NORM_L1 =>
            _normVal = 0.0
            groupIndices.foreach(featureIndex => _normVal += FastMath.abs(input(featureIndex)))
            normVal = if (_normVal == 0.0) 1.0 else _normVal
            groupIndices
              .foreach(featureIndex => input(featureIndex) = input(featureIndex) / normVal)
          case NORM_L2 =>
            _normVal = 0.0
            groupIndices.foreach(featureIndex => _normVal += pow2(input(featureIndex)))
            normVal = if (_normVal == 0.0) 1.0 else FastMath.sqrt(_normVal)
            groupIndices
              .foreach(featureIndex => input(featureIndex) = input(featureIndex) / normVal)
          case _ => // w/o normalization
        }

        groupNum += 1
      } // end foreach group
    } // end if groups.nonEmpty
  }

  @inline private def pow2(x: Double): Double = x * x
}

case class GroupedFeaturesTfidfTransformerConfig(
  groups: Array[Array[Int]] = Array.empty,
  idf_diags: Array[Array[Double]] = Array.empty,
  n_features: Int = 0,
  transformer_params: Map[String, String] = Map.empty,
)

object GroupedFeaturesTfidfTransformerConfig {
  val NORM_L1 = 1
  val NORM_L2 = 2
  val NO_NORM = 0
}
