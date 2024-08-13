/** Created by vasnake@gmail.com on 2024-07-24
  */
package com.github.vasnake.`etl-core`.aggregate

import scala.util.Try

import com.github.vasnake.common.num.FastMath

trait AggregationStage extends VectorTransformer[Double]

trait VectorTransformer[T] {
  def transform(vec: Array[T]): Array[T]
}

// Known set of stages

case class FilterDropNull() extends AggregationStage {
  def transform(vec: Array[Double]): Array[Double] = {
    vec.indices.foreach { idx =>
      // TODO: replace scala Double with java Double
      // if (vec(idx) == null) vec(idx) = Double.NaN // if there was null now it's 0.0 (scala Double works this way)
    }

    vec
  }
}

case class AggAverage(params: Map[String, String]) extends AggregationStage {
  // null: drop|3.14
  private val dropNull: Boolean = params.getOrElse("null", "drop").toLowerCase == "drop"
  private val nullReplacement: Double =
    Try(params.getOrElse("null", "drop").toDouble).getOrElse(Double.NaN)
  override def toString: String =
    s"AggAverage(params=${params}, dropNull=$dropNull, nullReplacement=$nullReplacement)"

  def transform(vec: Array[Double]): Array[Double] = {
    var sum: Double = 0
    var count: Int = 0

    vec.foreach { v =>
      if (!v.isNaN) {
        sum += v
        count += 1
      }
      else if (!dropNull && !nullReplacement.isNaN) {
        sum += nullReplacement
        count += 1
      }
    }

    if (count > 0) vec(0) = sum / count

    vec
  }
}

case class AggMostFreq(params: Map[String, String]) extends AggregationStage {
  //      "type": "agg", "name": "most_freq", "parameters": {"rnd_value": "0"}
  private val fixedRndIndex: Int =
    Try(params.getOrElse("rnd_value", "random").toDouble.toInt).getOrElse(Int.MinValue)
  override def toString: String = s"AggMostFreq(params=${params}, fixedRndIndex=$fixedRndIndex)"

  override def transform(vec: Array[Double]): Array[Double] = {
    // TODO: speed optimization
    if (fixedRndIndex < 0) FastMath.random()
    import scala.collection.mutable
    val table = mutable.HashMap.empty[Double, Int]

    vec.foreach(v => if (!v.isNaN) table.update(v, table.getOrElse(v, 0) + 1))

    val frequencies: Array[(Double, Int)] = table.toArray.sortWith {
      case (p1, p2) =>
        // p1 < p2; desc freqs but asc values
        if (p1._2 == p2._2) p1._1 < p2._1
        else p1._2 > p2._2
    }

    if (frequencies.nonEmpty)
      // not all values are nan
      if (frequencies.length < 2 || frequencies(0)._2 != frequencies(1)._2)
        // one top score
        vec(0) = frequencies.head._1
      else
        // a few top scores, select random item
        vec(0) = frequencies(selectRandomTopScoreIdx(frequencies, fixedRndIndex))._1

    vec
  }

  private def selectRandomTopScoreIdx(frequencies: Array[(Double, Int)], rnd: Int): Int =
    if (rnd < 0) {
      var numRecs: Int = 0
      frequencies.tail.foreach(pair => if (pair._2 == frequencies.head._2) numRecs += 1)
      FastMath.round(FastMath.random() * numRecs).toInt
    }
    else rnd // TODO: check for boundaries?
}

case class AggMax() extends Reducer(FastMath.max)
case class AggMin() extends Reducer(FastMath.min)
case class AggSum() extends Reducer(_ + _)

class Reducer(func: (Double, Double) => Double) extends AggregationStage {
  def transform(vec: Array[Double]): Array[Double] = {
    // TODO: optimize speed
    if (!vec.isEmpty) {
      var res = vec.head
      vec
        .tail
        .foreach(v =>
          if (!v.isNaN)
            if (res.isNaN) res = v
            else res = func(res, v)
        )

      vec(0) = res
    }

    vec
  }
}
