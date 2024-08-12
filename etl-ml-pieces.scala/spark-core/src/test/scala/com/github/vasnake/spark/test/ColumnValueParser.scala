/**
 * Created by vasnake@gmail.com on 2024-08-12
 */
package com.github.vasnake.spark.test

import com.github.vasnake.core.text.StringToolbox

object ColumnValueParser {
  import StringToolbox._
  import DefaultSeparators._

  def parseMapDouble(str: String): Option[Map[Option[String], Option[Double]]] = {
    // foo: 5, bar: 6
    if (str.trim.toLowerCase == "null") None
    else Some(str.splitTrim.map(kv => parseKV(kv)).toMap)
  }

  def parseArrayDouble(str: String): Option[Array[Option[Double]]] =
    if (str.trim.toLowerCase == "null") None
    else Some(str.splitTrim.map(x => parseDouble(x)))

  def parseStr(str: String): Option[String] =
    if (str.trim.toLowerCase == "null") None
    else Some(str)

  def parseDouble(x: String): Option[Double] = x.trim.toLowerCase match {
    case "" | "null" => None
    case "nan" => Some(Double.NaN)
    case "+inf" => Some(Double.PositiveInfinity)
    case "-inf" => Some(Double.NegativeInfinity)
    case validNumber => Some(validNumber.toDouble)
  }

  def parseKV(kv: String): (Option[String], Option[Double]) = {
    // foo: 5
    kv.splitTrim(commaColon.next.getOrElse(Separators(":"))) match {
      case Array(a, b) => parseStr(a) -> parseDouble(b)
      case _ => sys.error(s"Unknown pair encoding: `${kv}`")
    }
  }

}
