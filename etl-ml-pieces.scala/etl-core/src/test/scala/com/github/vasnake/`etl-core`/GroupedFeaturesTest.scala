/** Created by vasnake@gmail.com on 2024-08-08
  */
package com.github.vasnake.`etl-core`

//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

// testOnly *GroupedFeatures*
class GroupedFeaturesTest extends AnyFlatSpec with should.Matchers {
  it should "merge empty grouped features" in {
    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq.empty)

    assert(mergedGfs.groups.isEmpty)
  }

  it should "merge empty index" in {
    val gfs = GroupedFeatures(Seq(FeaturesGroup("colname", Array.empty)))
    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))

    assert(mergedGfs.groups.isEmpty)
  }

  it should "merge one group with one feature" in {
    val gfs = GroupedFeatures(Seq(FeaturesGroup("colname1", Array("idx11"))))
    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))

    assert(mergedGfs.groups.size === 1)
    assert(mergedGfs.featuresSize === 1)

    val group = mergedGfs.groups.head
    assert(group.name === "colname1")
    assert(group.index.toSeq === Array("idx11").toSeq)
  }

  it should "merge single GF with many features" in {
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
        FeaturesGroup("colname3", Array("idx31", "idx32", "idx33")),
      )
    )

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))

    assert(mergedGfs.groups.size === 3)
    assert(mergedGfs.featuresSize === 9)

    val groupsNames = mergedGfs.groups.map(_.name).sorted
    assert(groupsNames === Seq("colname1", "colname2", "colname3"))

    assert(mergedGfs.groupIndices("colname1").get.toSeq === Seq("idx11", "idx12", "idx13"))
    assert(mergedGfs.groupIndices("colname2").get.toSeq === Seq("idx21", "idx22", "idx23"))
    assert(mergedGfs.groupIndices("colname3").get.toSeq === Seq("idx31", "idx32", "idx33"))

    val featuresNames = mergedGfs.groups.flatMap(g => g.index).sorted
    assert(
      featuresNames === Seq(
        "idx11",
        "idx12",
        "idx13",
        "idx21",
        "idx22",
        "idx23",
        "idx31",
        "idx32",
        "idx33",
      )
    )
  }

  it should "merge single GF with duplicated features" in {
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx11")),
        FeaturesGroup("colname2", Array("idx22", "idx22", "idx23")),
      )
    )

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs))

    assert(mergedGfs.groups.size === 2)
    assert(mergedGfs.featuresSize === 4)

    val groupsNames = mergedGfs.groups.map(_.name).sorted
    assert(groupsNames === Seq("colname1", "colname2"))

    assert(mergedGfs.groupIndices("colname1").get.toSeq === Seq("idx11", "idx12"))
    assert(mergedGfs.groupIndices("colname2").get.toSeq === Seq("idx22", "idx23"))

    val featuresNames = mergedGfs.groups.flatMap(g => g.index).sorted
    assert(
      featuresNames === Seq(
        "idx11",
        "idx12",
        "idx22",
        "idx23",
      )
    )
  }

  it should "merge two GF with many features" in {
    val gfs1 = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx1", "idx2", "idx3")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
        FeaturesGroup("colname3", Array("idx31", "idx32", "idx33")),
      )
    )
    val gfs2 = GroupedFeatures(
      Seq(
        FeaturesGroup("colname4", Array("idx1", "idx2", "idx3")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
        FeaturesGroup("colname3", Array("idx31-2", "idx32", "idx33-2")),
      )
    )

    val mergedGfs = GroupedFeatures.mergeGroupedFeatures(Seq(gfs1, gfs2))

    assert(mergedGfs.groups.size === 4)
    assert(mergedGfs.featuresSize === 14)

    val groupsNames = mergedGfs.groups.map(_.name).sorted
    assert(groupsNames === Seq("colname1", "colname2", "colname3", "colname4"))

    assert(mergedGfs.groupIndices("colname1").get.toSeq === Seq("idx1", "idx2", "idx3"))
    assert(mergedGfs.groupIndices("colname2").get.toSeq === Seq("idx21", "idx22", "idx23"))
    assert(
      mergedGfs.groupIndices("colname3").get.toSeq === Seq(
        "idx31",
        "idx31-2",
        "idx32",
        "idx33",
        "idx33-2",
      )
    )
    assert(mergedGfs.groupIndices("colname4").get.toSeq === Seq("idx1", "idx2", "idx3"))

    val featuresNames = mergedGfs.groups.flatMap(g => g.index).sorted
    assert(
      featuresNames === Seq(
        "idx1",
        "idx1",
        "idx2",
        "idx2",
        "idx21",
        "idx22",
        "idx23",
        "idx3",
        "idx3",
        "idx31",
        "idx31-2",
        "idx32",
        "idx33",
        "idx33-2",
      )
    )
  }

  it should "compute ordered indices in one group" in {
    val mergedGfs = GroupedFeatures(Seq(FeaturesGroup("colname", Array("idx1", "idx2", "idx3"))))
    val gfs = GroupedFeatures(Seq(FeaturesGroup("colname", Array("idx1", "idx2", "idx3"))))
    val expected = Array(0, 1, 2)

    val res = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    assert(res.toSeq === expected.toSeq)
  }

  it should "compute unordered indices in one group" in {
    val mergedGfs = GroupedFeatures(Seq(FeaturesGroup("colname", Array("idx1", "idx2", "idx3"))))
    val gfs = GroupedFeatures(Seq(FeaturesGroup("colname", Array("idx3", "idx1", "idx2"))))
    val expected = Array(2, 0, 1)

    val res = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    assert(res.toSeq === expected.toSeq)
  }

  it should "compute ordered indices in two groups" in {
    val mergedGfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
      )
    )
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
      )
    )
    val expected = Array(0, 1, 2, 3, 4, 5)

    val res = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    assert(res.toSeq === expected.toSeq)
  }

  it should "compute unordered indices in two groups" in {
    val mergedGfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
      )
    )
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname2", Array("idx23", "idx21", "idx22")),
        FeaturesGroup("colname1", Array("idx11", "idx13", "idx12")),
      )
    )
    val expected = Array(5, 3, 4, 0, 2, 1)

    val res = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    assert(res.toSeq === expected.toSeq)
  }

  it should "compute indices for features subset" in {
    val mergedGfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
      )
    )
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname2", Array("idx23", "idx21"))
      )
    )
    val expected = Array(5, 3)

    val res = GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    assert(res.toSeq === expected.toSeq)
  }

  it should "throw IndexOutOfBounds computing indices for absent group" in {
    val mergedGfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
      )
    )
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname3", Array("idx33", "idx31"))
      )
    )

    assertThrows[IndexOutOfBoundsException] {
      GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    }
  }

  it should "throw IndexOutOfBounds computing index for absent feature" in {
    val mergedGfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx11", "idx12", "idx13")),
        FeaturesGroup("colname2", Array("idx21", "idx22", "idx23")),
      )
    )
    val gfs = GroupedFeatures(
      Seq(
        FeaturesGroup("colname1", Array("idx14"))
      )
    )

    assertThrows[IndexOutOfBoundsException] {
      GroupedFeatures.computeFeaturesIndices(mergedGfs, gfs)
    }
  }

  it should "slice empty vector from empty vector" in {
    val features: Array[Double] = Array.empty
    val indices: Array[Int] = Array.empty
    val res: Array[Double] = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.isEmpty)
  }

  it should "slice empty vector by empty indices" in {
    val features = GroupedFeaturesSliceTest.features
    val indices: Array[Int] = Array.empty
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.isEmpty)
  }

  it should "slice empty vector from empty features" in {
    val features: Array[Double] = Array.empty
    val indices = Array(0, 1, 2, 3, 4, 5, 6, 7)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.isEmpty)
  }

  it should "slice ordered full vector" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(0, 1, 2, 3, 4, 5, 6, 7)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === GroupedFeaturesSliceTest.features.toSeq)
  }

  it should "slice ordered chunk from the beginning" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(0, 1, 2)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === GroupedFeaturesSliceTest.features.toSeq.take(3))
  }

  it should "slice ordered chunk till the end" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(5, 6, 7)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === GroupedFeaturesSliceTest.features.toSeq.takeRight(3))
  }

  it should "slice ordered chunk in the middle" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(3, 4, 5)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === GroupedFeaturesSliceTest.features.toSeq.slice(3, 6))
  }

  it should "slice ordered reverse chunk" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(7, 6, 5, 4, 3, 2, 1, 0)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === GroupedFeaturesSliceTest.features.toSeq.reverse)
  }

  it should "slice unordered chunk from the beginning" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(2, 0, 1)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === Seq(0.3, 0.1, 0.2))
  }

  it should "slice unordered chunk till the end" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(5, 7, 6)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === Seq(0.97, 0.95, 0.96))
  }

  it should "slice unordered chunk in the middle" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(5, 4, 6)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === Seq(0.97, 0.98, 0.96))
  }

  it should "slice unordered scattered chunk" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(5, 2, 7, 1)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === Seq(0.97, 0.3, 0.95, 0.2))
  }

  it should "slice duplicated items" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(1, 1, 0, 1)
    val res = GroupedFeatures.sliceFeaturesVector(features, indices)
    assert(res.toSeq === Seq(0.2, 0.2, 0.1, 0.2))
  }

  it should "throw OutOfBounds" in {
    val features = GroupedFeaturesSliceTest.features
    val indices = Array(0, 1, 8)
    assertThrows[IndexOutOfBoundsException] {
      GroupedFeatures.sliceFeaturesVector(features, indices)
    }
  }
}

object GroupedFeaturesSliceTest {
  val features = Array(0.1, 0.2, 0.3, 0.99, 0.98, 0.97, 0.96, 0.95)
}
