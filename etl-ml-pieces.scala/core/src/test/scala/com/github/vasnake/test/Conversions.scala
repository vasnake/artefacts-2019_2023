/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.test

import com.github.vasnake.test.EqualityCheck.createSeqFloatsEquality
import org.scalactic._

object Conversions {
  def floatsAreEqual(
    a: Float,
    b: Float,
    tolerance: Float,
  ): Boolean =
    (a <= b + tolerance) && (a >= b - tolerance)

  object implicits {
    import scala.reflect.ClassTag
    import scala.language.implicitConversions

    implicit def seq2array[T: ClassTag](lst: Seq[T]): Array[T] = lst.toArray

    implicit class SeqOfDouble(xs: Iterable[Double]) {
      def toFloat: Seq[Float] = xs.toSeq.map(_.toFloat)
      def toString(accuracy: Int): Seq[String] = xs.toSeq.map(x => s"%1.${accuracy}f".format(x))
    }

    implicit class SeqOfFloats(xs: Seq[Float]) {
      def toDouble: Seq[Double] = xs.map(_.toDouble)
    }

    implicit def arrayD2arrayF(ds: Array[Double]): Array[Float] = ds.map(_.toFloat)
    implicit def arrayF2arrayD(fs: Array[Float]): Array[Double] = fs.map(_.toDouble)
  }

  object equalityImplicits {
    implicit val tolerance: Float = 0.00001f
    implicit val seqFloatEquals: Equality[Seq[Float]] =
      createSeqFloatsEquality((a, b) => floatsAreEqual(a, b, tolerance))
    implicit val floatEquals: Equality[Float] = TolerantNumerics.tolerantFloatEquality(tolerance)
  }
}
