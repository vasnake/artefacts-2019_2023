/** Created by vasnake@gmail.com on 2024-07-11
  */
package com.github.vasnake.`etl-core`

import scala.reflect.ClassTag

import com.github.vasnake.core.num.VectorToolbox

/** Features dictionary for ML models: represents mapping from DataFrame column to features vector.
  * DataFrame column should be of type Map or Array, column as a collection of features.
  *
  * @param name source DataFrame column name
  * @param index list of keys for map or array stored in column; values under that keys are features
  * @param isArray type of dataframe column: Map or Array
  */
case class FeaturesGroup(
  name: String,
  index: Array[String],
  isArray: Boolean = false,
) {
  override def toString: String =
    s"""FeaturesGroup("${name}", Array(${index.mkString(", ")}), isArray = ${isArray})"""
}

case class GroupedFeatures(groups: Seq[FeaturesGroup]) {
  def groupNumber(name: String): Option[Int] = GroupedFeatures.groupNumber(groups, name)
  def groupIndices(name: String): Option[Array[String]] =
    GroupedFeatures.groupNumber(groups, name).map(groups(_).index)
  def featuresSize: Int = GroupedFeatures.featuresSize(groups)

  override def toString: String = s"""GroupedFeatures(Seq(${groups.mkString(",")}))"""
}

object GroupedFeatures {
  def sliceFeaturesVector[@specialized(Double, Float) T: ClassTag](
    features: Array[T],
    indices: Array[Int],
  ): Array[T] =
    VectorToolbox.selectIndexed(features, indices)

  def computeFeaturesIndices(mergedGfs: GroupedFeatures, gfs: GroupedFeatures): Array[Int] = {
    val res = (gfs.groups foldLeft Vector.empty[Int]) { (vector, group) =>
      val groupNumber = mergedGfs
        .groupNumber(group.name)
        .getOrElse(
          throw new IndexOutOfBoundsException(
            s"can't find group '${group.name}' in merged groups '${mergedGfs.groups.map(_.name)}'"
          )
        )
      val mergedGroup = mergedGfs.groups(groupNumber)
      val name2index = mergedGroup.index.zipWithIndex.toMap
      val prefixSize = featuresSize(mergedGfs.groups.take(groupNumber))

      val groupIndices = group
        .index
        .map(name =>
          name2index.getOrElse(
            name,
            throw new IndexOutOfBoundsException(
              s"can't find feature name '${name}' in group '${mergedGroup}'"
            ),
          ) + prefixSize
        )

      vector ++ groupIndices.toVector
    }

    res.toArray
  }

  def mergeGroupedFeatures(gfs: Seq[GroupedFeatures]): GroupedFeatures =
    if (gfs.isEmpty || gfs.head.groups.isEmpty || gfs.head.groups.head.index.isEmpty)
      GroupedFeatures(Seq.empty)
    else {
      val res = (gfs foldLeft Map.empty[String, Array[String]])((coll, gf) => merge(coll, gf))
      GroupedFeatures(res.map { case (name, lst) => FeaturesGroup(name, lst.sorted) }.toSeq)
    }

  private def featuresSize(groups: Seq[FeaturesGroup]): Int =
    (groups foldLeft 0)((acc, itm) => acc + itm.index.length)

  private def groupNumber(groups: Seq[FeaturesGroup], name: String): Option[Int] = {
    val idx = groups.prefixLength(_.name != name)
    if (idx < groups.size) Some(idx)
    else None
  }

  private def updateGroupedFeatures(groupedFeatures: Map[String, Array[String]], fg: FeaturesGroup)
    : Map[String, Array[String]] =
    if (groupedFeatures.isDefinedAt(fg.name))
      groupedFeatures.updated(
        fg.name,
        (groupedFeatures(fg.name) ++ fg.index).distinct,
      )
    else groupedFeatures + (fg.name -> fg.index.distinct)

  private def merge(groupedFeatures: Map[String, Array[String]], itm: GroupedFeatures)
    : Map[String, Array[String]] =
    (itm.groups foldLeft groupedFeatures)((gfs, group) => updateGroupedFeatures(gfs, group))
}
