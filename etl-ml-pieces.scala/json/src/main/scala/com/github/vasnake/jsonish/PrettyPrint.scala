/** Created by vasnake@gmail.com on 2024-07-25
  */
package com.github.vasnake.jsonish

import com.github.vasnake.json.{ JsonToolbox => jtb }

/** First attempt to implement some stupid idea about 'pretty print' for set of data structures.
  * By default printing ML models and configs to log looks ugly and un-readable.
  * I wanted something like a type-class for functionality similar to 'toString', using json format.
  */
object PrettyPrint {
  trait Jsonifier[-A] {
    def doJsonification(x: A): String
  }
  def jsonify[A](
    x: A
  )(implicit
    cap: Jsonifier[A]
  ): String = cap.doJsonification(x)

  object implicits {
    implicit val jstring: Jsonifier[String] = new Jsonifier[String] {
      override def doJsonification(x: String): String = jtb.writeObject(x, jtb.Pretty)
    }

    implicit val joption: Jsonifier[Option[_]] = new Jsonifier[Option[_]] {
      override def doJsonification(x: Option[_]): String = x match {
        case None => jsonify("None")
        case Some(r: AnyRef) => jtb.writeObject(r, jtb.Pretty)
        case Some(v) => s"$v"
      }
    }

    implicit val jlist: Jsonifier[List[_]] = new Jsonifier[List[_]] {
      override def doJsonification(xs: List[_]): String = xs
        .map {
          case x: AnyRef => jtb.writeObject(x)
          case v => s"$v"
        }
        .mkString("[", ",", "]")
    }

    implicit val jmap: Jsonifier[Map[_, _]] = new Jsonifier[Map[_, _]] {
      override def doJsonification(xs: Map[_, _]): String = xs
        .map {
          case x: AnyRef => jtb.writeObject(x)
          case v => s"$v"
        }
        .mkString("[", ",", "]")
    }

    implicit val jarray: Jsonifier[Array[_]] = new Jsonifier[Array[_]] {
      override def doJsonification(xs: Array[_]): String = xs
        .map {
          case x: AnyRef => jtb.writeObject(x)
          case v => s"$v"
        }
        .mkString("[", ",", "]")
    }

    implicit val jtraversable: Jsonifier[Traversable[_]] = new Jsonifier[Traversable[_]] {
      override def doJsonification(xs: Traversable[_]): String = jsonify(xs.toList)
    }
  }
}
