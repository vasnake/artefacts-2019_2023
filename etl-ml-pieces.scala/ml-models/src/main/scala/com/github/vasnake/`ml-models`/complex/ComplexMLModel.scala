/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`ml-models`.complex

import com.github.vasnake.`etl-core`.GroupedFeatures

trait ComplexMLModel {
  def isOK: Boolean = true
  def message: String = "placeholder for message in case if !model.isOK"
  def groupedFeatures: GroupedFeatures // features indices, for features selection from features storage
  def apply(features: Array[Double]): Seq[Any] // predicted values, output record predicted on input record of features
}

case class InvalidModel(description: Map[String, String], override val message: String)
    extends ComplexMLModel {
  override def isOK: Boolean = false
  override def apply(features: Array[Double]): Seq[Any] = ???
  override def groupedFeatures: GroupedFeatures = ???

  override def toString: String =
    s"InvalidModel(description = ${description}, message = ${message})"
}

case class BaseLineModel(description: Map[String, String]) extends ComplexMLModel {
  private val audienceName = description.getOrElse("audience_name", "baseline")

  override def apply(features: Array[Double]): Seq[Any] =
    Seq(
      0.5, // equalized score value
      Array(0.5), // raw scores array
      Array(0.5), // equalized scores array
      audienceName, // project id (target)
      "positive", // category
    )

  override def groupedFeatures: GroupedFeatures = GroupedFeatures(groups = Seq.empty)
}
