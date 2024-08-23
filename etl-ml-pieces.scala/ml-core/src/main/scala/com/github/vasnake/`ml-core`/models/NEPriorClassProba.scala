/** Created by vasnake@gmail.com on 2024-07-29
  */
package com.github.vasnake.`ml-core`.models

import scala.collection.mutable
import scala.util._

import com.github.vasnake.common.num.{ FastMath => fm }

case class NEPriorClassProba(aligner: Array[Double]) {
  /** Mutate probs array
    *
    * @param probs
    */
  def transform(probs: Array[Double]): Unit = {
    // TODO: consider using Vectors from ml.linalg
    // all checks performed already?
    // TODO: add optional checks

    val sum = probs.sum
    var newSum: Double = 0.0

    probs.indices.foreach { i =>
      val nx = probs(i) / sum * aligner(i)
      newSum += nx
      probs(i) = nx
    }

    probs.indices.foreach { i => // TODO: use while(i < probs.length) loop
      probs(i) = probs(i) / newSum
    }
  }
}

object NEPriorClassProba {

  // TODO: use case class (message, option aligners)
  def fit(
    probs: Iterator[Array[Double]],
    numClasses: Int,
    priorValues: Array[Double]
  ): (String, Option[Array[Double]]) = Try {
    // normalize prior
    // normalize probs
    // grid search: compute list of aligners and select the best

    // prepare input

    require(
      priorValues.length == numClasses,
      "Prior values count must be equal to number of classes"
    )
    require(priorValues.min > 0, "Prior values mast be positive")

    val rows: Array[Array[Double]] = {
      val builder = mutable.ArrayBuilder.make[Array[Double]]()
      // builder.sizeHint(100000) // TODO: pass sample size in parameter
      probs.foreach { row =>
        val sum = row.sum
        builder += row.map(_ / sum)
      }

      builder.result()
    }
    require(rows.length > 0, "At least one record required to fit aligner")

    val prior: Array[Double] = {
      val sum = priorValues.sum
      priorValues.map(_ / sum)
    }
    val counts: Array[Double] = prior.map(_ * rows.length) // TODO: combine with calc. of prior

    val grid: Iterator[(Double, Double, Double)] = makeGrid.iterator

    // search for best aligner

    var loss: Double = Double.MaxValue
    val aligners = mutable.ArrayBuffer.empty[(Array[Double], Double)]
    while (grid.hasNext && loss >= 0.001) { // early stop
      val (lr, lr_decay, momentum) = grid.next()

      aligners += getAligner(rows, numClasses, counts, lr, lr_decay, momentum)

      loss = aligners.last._2
    }
    require(aligners.nonEmpty, "Grid search must produce some results")

    ("", Some(aligners.minBy(_._2)._1))
  } match {
    case Success(res) => res
    case Failure(err) => (err.getMessage, None)
  }

  def makeGrid: Iterable[(Double, Double, Double)] = {
    // TODO: add tests, make it lazy, move to (scipy? iterators?) lib
    // https://stackoverflow.com/questions/8321906/lazy-cartesian-product-of-several-seqs-in-scala/8569263
    //   grid = [  # it.product([0.5, 0.1], [0.7, 0.9], [0.3, 0.9])
    //    (0.5, 0.7, 0.3),
    //    (0.5, 0.7, 0.9),
    //    (0.5, 0.9, 0.3),
    //    (0.5, 0.9, 0.9),
    //    (0.1, 0.7, 0.3),
    //    (0.1, 0.7, 0.9),
    //    (0.1, 0.9, 0.3),
    //    (0.1, 0.9, 0.9)
    //  ]

    def cartesian[A](list: List[List[A]]): List[List[A]] =
      list match {
        case Nil => List(List())
        case h :: t => h.flatMap(i => cartesian(t).map(i :: _))
      }

    cartesian(List(List(0.5, 0.1), List(0.7, 0.9), List(0.3, 0.9))).map {
      case a :: b :: c :: Nil => (a, b, c)
      case _ => throw new IllegalArgumentException("Grid must contain 3 numbers in every row")
    }
  }

  def getAligner(
    rows: Array[Array[Double]],
    n_classes: Int,
    counts: Array[Double],
    lr_start: Double,
    lr_decay: Double,
    momentum: Double,
    max_iter: Int = 1000,
    eps: Double = 1e-8
  ): (Array[Double], Double) = {
    val n_rows = rows.length

    var dc_n: Double = 0.0
    var old_dc_n: Double = 0.0
    var lr: Double = lr_start

    // TODO: should allocate memory only once
    val pred: Array[Int] = Array.fill(n_rows)(0)
    val aligner: Array[Double] = Array.fill(n_classes)(1.0)
    val velocity: Array[Double] = Array.fill(n_classes)(0.0)
    val dc_v: Array[Double] = Array.fill(n_classes)(0.0)
    val buff: Array[Double] = new Array[Double](n_classes)

    var iter = 0
    while (iter < max_iter && lr >= eps && (dc_n != 0 || iter == 0)) {
      // compute dc_v
      pred.indices.foreach { i => // pred: Vector(n_rows) = np.argmax(X * aligner, axis=1)
        mul(rows(i), aligner, buff)
        pred(i) = argmax(buff)
      }
      dc_v.indices.foreach { i =>
        val uy = counts(i)
        val up = pred.count(_ == i) // up = (pred == c).sum()  # num of rows where c == argmax; [0, n_rows)
        dc_v(i) = (uy - up) / (uy + 1.0)
      }

      // update coefficients
      old_dc_n = dc_n
      dc_n = sum_abs(dc_v) // dc_n = np.abs(dc_v).sum()
      if (dc_n > old_dc_n) lr *= lr_decay

      // if lr < eps or dc_n == 0.0: break
      if (lr >= eps && dc_n != 0) {
        div(dc_v, dc_n, dc_v)
        mul(dc_v, lr, dc_v)
        mul(velocity, momentum, velocity)
        sum(velocity, dc_v, velocity) // velocity = (velocity * momentum) + (lr * (dc_v / dc_n))
        sum(aligner, velocity, aligner) // aligner += velocity
      }

      iter += 1
    }

    (aligner, dc_n / n_classes)
  }

  // TODO: move functions below to numpy package

  @inline def sum_abs(xs: Array[Double]): Double = {
    var res: Double = 0

    var i = 0
    while (i < xs.length) {
      res += fm.abs(xs(i))
      i += 1
    }

    res
  }

  @inline def argmax(xs: Array[Double]): Int = {
    // import breeze.linalg.argmax
    // Vectors.dense(xs).argmax
    var res: Int = 0

    var max = xs.head
    var i: Int = 1
    while (i < xs.length) {
      if (xs(i) > max) {
        max = xs(i)
        res = i
      }
      i += 1
    }

    res
  }

  @inline def mul(
    xs: Array[Double],
    ys: Array[Double],
    res: Array[Double]
  ): Unit = {
    assert(xs.length == ys.length && xs.length == res.length)
    var i = 0
    while (i < xs.length) {
      res(i) = xs(i) * ys(i)
      i += 1
    }
  }

  @inline def mul(
    xs: Array[Double],
    y: Double,
    res: Array[Double]
  ): Unit = {
    assert(xs.length == res.length)
    var i = 0
    while (i < xs.length) {
      res(i) = xs(i) * y
      i += 1
    }
  }

  @inline def sum(
    xs: Array[Double],
    ys: Array[Double],
    res: Array[Double]
  ): Unit = {
    assert(xs.length == ys.length && xs.length == res.length)
    var i = 0
    while (i < xs.length) {
      res(i) = xs(i) + ys(i)
      i += 1
    }
  }

  @inline def div(
    xs: Array[Double],
    y: Double,
    res: Array[Double]
  ): Unit = {
    assert(xs.length == res.length)
    var i = 0
    while (i < xs.length) {
      res(i) = xs(i) / y
      i += 1
    }
  }
}
