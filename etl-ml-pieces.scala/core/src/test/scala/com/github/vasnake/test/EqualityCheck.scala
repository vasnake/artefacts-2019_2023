/** Created by vasnake@gmail.com on 2024-08-12
  */
package com.github.vasnake.test

import java.lang.{ Double => jDouble }

import org.scalactic.Equality

object EqualityCheck {
  def createSeqFloatsEquality(floatsEqual: (Float, Float) => Boolean): Equality[Seq[Float]] =
    new Equality[Seq[Float]] {
      def areEqual(as: Seq[Float], bs: Any): Boolean = bs match {
        case bSeq: Seq[_] =>
          as.size == bSeq.size && as.zip(bSeq).forall {
            case (a: Float, b: Float) => floatsEqual(a, b)
            case _ => false
          }
        case _ => false
      }
      override def toString: String = s"SeqFloatsEquality"
    }

  def createDoubleEquality(equal: (Double, Double) => Boolean): Equality[Double] =
    new Equality[Double] {
      def areEqual(a: Double, b: Any): Boolean =
        b match {
          case d: Double => equal(a, d)
          case f: Float => equal(a, f.toDouble)
          case xz => equal(a, xz.toString.toDouble)
        }
      override def toString: String = s"DoubleEquality"
    }

  def createJavaDoubleEquality(equal: (Double, Double) => Boolean): Equality[jDouble] =
    new Equality[jDouble] {
      def areEqual(a: jDouble, b: Any): Boolean =
        b match {
          case d: Double => equal(a, d)
          case f: Float => equal(a, f.toDouble)
          case xz => equal(a, xz.toString.toDouble)
        }
      override def toString: String = s"JavaDoubleEquality"
    }
}
