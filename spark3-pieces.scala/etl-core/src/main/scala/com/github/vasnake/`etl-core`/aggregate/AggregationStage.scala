/** Created by vasnake@gmail.com on 2024-07-24
  */
package com.github.vasnake.`etl-core`.aggregate

import scala.util.Try
import scala.collection.mutable
import java.lang.Double.{isNaN, isInfinite}

import com.github.vasnake.common.num.FastMath

trait VectorTransformer[T] {
  def transform(vec: Array[T]): Array[T]
}

trait AggregationStage extends VectorTransformer[Double]

// Known set of stages

case class ReplaceInvalid2Zero() extends AggregationStage {
  def transform(vec: Array[Double]): Array[Double] = {
    vec.indices.foreach { idx =>
      if (isNaN(vec(idx)) || isInfinite(vec(idx))) vec(idx) = 0.0
    }

    vec
  }
}

case class AggAverage(params: Map[String, String]) extends AggregationStage {
  // `null: drop` or `null: 3.14`
  private val dropNull: Boolean = params.getOrElse("null", "drop").toLowerCase == "drop"
  private val nullReplacement: Double = Try(params.getOrElse("null", "drop").toDouble).getOrElse(0.0)

  override def toString: String = s"AggAverage(params=${params}, dropNull=$dropNull, nullReplacement=$nullReplacement)"

  def transform(vec: Array[Double]): Array[Double] = {
    var sum: Double = 0
    var count: Int = 0

    vec.foreach { v =>
      if (isNaN(v) || isInfinite(v)) {
        if (!dropNull) {
          sum += nullReplacement
          count += 1
        }
      }
      else {
        sum += v
        count += 1
      }
    }

    if (count > 0) vec(0) = sum / count // save result

    vec
  }
}

case class AggMostFreq(params: Map[String, String]) extends AggregationStage {
  // "type": "agg", "name": "most_freq", "parameters": {"rnd_value": "0"}
  private val fixedRndIndex: Int = Try(params.getOrElse("rnd_value", "random").toDouble.toInt).getOrElse(Int.MinValue)

  override def toString: String = s"AggMostFreq(params=${params}, fixedRndIndex=$fixedRndIndex)"

  override def transform(vec: Array[Double]): Array[Double] = {
    if (fixedRndIndex < 0) FastMath.random() // kick rnd
    // value -> count
    val table = mutable.HashMap.empty[Double, Int] // bad idea, map key should not rely on floating point implementation

    vec.foreach(v =>
      if (isNaN(v) || isInfinite(v)) () else table.update(v, table.getOrElse(v, 0) + 1)
    )

    val frequencies: Array[(Double, Int)] = table.toArray.sortWith {
      case (pair1, pair2) =>
        // p1 < p2; desc freqs but asc values
        if (pair1._2 == pair2._2) pair1._1 < pair2._1
        else pair1._2 > pair2._2
    }

    if (frequencies.nonEmpty)
      if (frequencies.length < 2 || frequencies(0)._2 != frequencies(1)._2) // not all values are nan
        vec(0) = frequencies.head._1 // one top score
      else
        vec(0) = frequencies(selectRandomTopScoreIdx(frequencies, fixedRndIndex))._1 // a few top scores, select random item

    vec
  }

  private def selectRandomTopScoreIdx(frequencies: Array[(Double, Int)], rnd: Int): Int =
    if (rnd < 0) {
      var numRecs: Int = 0
      frequencies.tail.foreach(pair => if (pair._2 == frequencies.head._2) numRecs += 1)
      FastMath.round(FastMath.random() * numRecs).toInt
    }
    else FastMath.min(rnd, frequencies.length - 1).toInt
}

case class AggMax() extends Reducer(FastMath.max)
case class AggMin() extends Reducer(FastMath.min)
case class AggSum() extends Reducer(_ + _)

class Reducer(func: (Double, Double) => Double) extends AggregationStage {

  private val f: (Double, Double) => Double = (x, y) => {
    if (isNaN(x) || isInfinite(x)) y
    else if (isNaN(y) || isInfinite(y)) x
    else func(x, y)
  }

  def transform(vec: Array[Double]): Array[Double] = {
    if (!vec.isEmpty) vec(0) = vec reduce f

    vec
  }
}
