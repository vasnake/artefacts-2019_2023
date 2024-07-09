/**
 * Created by vasnake@gmail.com on 2024-07-09
 */
package com.github.vasnake.core.text

import scala.util.Try
import java.util.Base64
import java.nio.charset.StandardCharsets

/**
 * Useful string operations, pimp-my-library style
 */
object StringToolbox {

  /**
   * String separators used in RichString class
   * @param v first level separator
   * @param next rest of the separators list
   */
  case class Separators(v: String, next: Option[Separators] = None)

  case class B64CharsMapping(mapping: Map[Char, Char]) {
    def map(c: Char): Char = mapping.getOrElse(c, c)
  }

  /**
   * base64 extra chars mapping from python implementation
   * https://tools.ietf.org/html/rfc3548
   */
  object DefaultB64Mapping {
    implicit val extraCharsMapping: B64CharsMapping = B64CharsMapping(Map('-' -> '+', '_' -> '/'))
  }

  /**
   * Default implicit values for string separators.
   * Implicit string-to-separator conversion.
   */
  object DefaultSeparators {
    implicit val commaColon: Separators = Separators(",", Some(Separators(":")))

    import scala.language.implicitConversions
    implicit def stringToSeparators(sep: String): Separators = Separators(sep)
  }

  implicit class RichString(val src: String) extends AnyVal {

    /**
     * Split string by separator, take item in `pos` position and convert it to double
     * @param pos zero-based item position in the string
     * @param sep splitting marker
     * @return a parsed number or None
     */
    def extractNumber(pos: Int)(implicit  sep: Separators): Option[Double] = {
      Try { src.splitTrim(sep)(pos).toDouble }.toOption
    }

    /**
     * Convert string to array of trimmed strings, empty items will be filter out.
     * @param sep split marker
     * @return empty array or array of trimmed strings
     */
    def splitTrim(implicit sep: Separators): Array[String] =
      src.trim.split("""\s*""" + sep.v + """\s*""").filter(_.nonEmpty)

    /**
     * Convert string to Seq of trimmed strings, empty items will be filtered out.
     * Splitting don't use regexp, and result converted from array to a sequence.
     * @param sep split marker
     * @return empty Seq or Seq of trimmed strings
     */
    def s2list(implicit sep: Separators): Seq[String] =
      src.split(sep.v).map(_.trim).filter(_.nonEmpty)

    /**
     * Convert string to Seq of trimmed strings, no filters applied.
     * @param sep split marker
     * @return Seq of trimmed strings
     */
    def splitTrimNoFilter(implicit sep: Separators): Seq[String] = {
      // TODO: add unit tests
      @scala.annotation.tailrec
      def loop(text: String, sep: String, acc: Seq[String]): Seq[String] = {
        val idx = text.indexOf(sep)
        if (idx < 0) acc :+ text.trim
        else {
          val item = text.substring(0, idx)
          val tail = text.substring(idx + sep.length)
          loop(tail, sep, acc :+ item.trim)
        }
      }

      loop(src, sep.v, Seq.empty[String])
    }

    /**
     * Create Map from string, e.g. "foo: bar; poo: bazz"
     */
    def parseMap(implicit sep: Separators): Map[String, String] = {
      val kvsep = sep.next.getOrElse(Separators(":"))
      val res = for {
        Array(k, v) <- src.splitTrim(sep).map(_.splitTrim(kvsep))
      } yield k -> v

      res.toMap
    }

    /**
     * Decode base64 text to utf-8 string
     * @param mapping extra chars mapping, according to RFC used to encode
     * @return
     */
    def b64Decode(implicit mapping: B64CharsMapping): String = {
      // TODO: add tests
      new String(
        Base64.getDecoder.decode(src.map(mapping.map)),
        StandardCharsets.UTF_8
      )
    }

    def b64Encode(): String = {
      // TODO: add tests
      new String(
        Base64.getEncoder.encode(src.getBytes(StandardCharsets.UTF_8)),
        StandardCharsets.UTF_8
      )
    }

    def repr(obj: Any): String = {
      if (obj == null) "null"
      else s"value `${obj.toString}` of type `${obj.getClass.getSimpleName}`"
    }
  }
}
