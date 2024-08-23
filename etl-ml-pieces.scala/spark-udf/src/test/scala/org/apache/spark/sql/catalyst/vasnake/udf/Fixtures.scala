/** Created by vasnake@gmail.com on 2024-08-13
  */
package org.apache.spark.sql.catalyst.vasnake.udf

import scala.util.Try

import com.github.vasnake.spark.test.ColumnValueParser
import org.apache.spark.sql._

object Fixtures {

  // Next time try more declarative tests
  def createInputDF(spark: SparkSession): DataFrame =
    // row: (part, uid, feature, feature_list, feature_map)
    spark.createDataFrame(
      Seq(
        // smoke test
        InputRow("A", "a", "1", "2, 3, 4", "foo: 5, bar: 6"),

        // produce null if all values are in (null, nan, inf), part B
        InputRow("B", "a", "null", "", ""),
        InputRow("B", "b", "+inf", "", ""),
        InputRow("B", "c", "nan", "", ""),
        InputRow("B", "d", "-inf", "", ""),
        InputRow("B", "e", "nan", "", ""),
        InputRow("B", "f", "nan", "", ""),

        // ignore (inf, null, nan) if valid values exists, part D, primitives
        InputRow("D", "a", "null", "", ""),
        InputRow("D", "b", "0", "", ""),
        InputRow("D", "c", "0", "", ""),
        InputRow("D", "d", "-inf", "", ""),
        InputRow("D", "e", "+inf", "", ""),
        InputRow("D", "f", "4", "", ""),
        InputRow("D", "g", "3", "", ""),
        InputRow("D", "h", "nan", "", ""),
        InputRow("D", "i", "nan", "", ""),
        InputRow("D", "j", "nan", "", ""),

        // produce null if all collections are null, part E
        InputRow("E", "a", "", "null", "null"),
        InputRow("E", "b", "", "null", "null"),

        // produce empty collection if all collections are null or empty, part F
        InputRow("F", "a", "", "null", "null"),
        InputRow("F", "b", "", "", ""),

        // produce null item if all items are in (null, nan, inf), part G, collections
        InputRow("G", "a", "", "null,  null", "1:null, 2:nan"),
        InputRow("G", "b", "", "3,     +inf", "1:3, 2:nan"),
        InputRow("G", "c", "", "nan,   -inf", "2:nan"),
        InputRow("G", "d", "", "4,     null", "1:4, 2:+inf"),
        InputRow("G", "e", "", "-inf,  nan", "2:null"),
        InputRow("G", "f", "", "+inf,  null", "2:-inf"),

        // ignore (inf, null, nan) if valid values exists, part I, collections
        InputRow("I", "a", "", "-inf,    3", "1:null, 2:3"),
        InputRow("I", "b", "", "3,     nan", "1:3, 2:nan"),
        InputRow("I", "c", "", "nan,  +inf", "1:nan, 2:null"),
        InputRow("I", "d", "", "4,    -inf", "1:4, 2:-inf"),
        InputRow("I", "e", "", "null,    2", "1:+inf, 2:2"),
        InputRow("I", "f", "", "+inf, null", "1:-inf, 2:+inf"),

        // consider absent values as null, part J
        InputRow("J", "a", "", "", ""),
        InputRow("J", "b", "", "1", "1:1"),
        InputRow("J", "c", "", "null, 2", "2:2"),
        InputRow("J", "d", "", "null, null, 3", "3:3"),

        // process string keys, part K
        InputRow("K", "a", "", "", ""),
        InputRow("K", "b", "", "", "null"),
        InputRow("K", "c", "", "", "1:nan"),
        InputRow("K", "d", "", "", "foo:nan"),
        InputRow("K", "e", "", "", "1:0.5, foo:1.7"),
        InputRow("K", "f", "", "", "1:null"),
        InputRow("K", "g", "", "", "foo:null"),
        InputRow("K", "h", "", "", "1:0.5, foo:0.3"),
        InputRow("K", "i", "", "", "1:null, foo:nan"),
        InputRow("K", "j", "", "", "1:nan, foo:null"),

        // most frequent values part L
        InputRow("L", "b", "null", "", ""),
        InputRow("L", "c", "-inf", "", ""),
        InputRow("L", "d", "0", "", ""),
        InputRow("L", "e", "null", "", ""),
        InputRow("L", "f", "nan", "", ""),
        InputRow("L", "g", "+inf", "", ""),
        InputRow("L", "h", "0", "", ""),
        InputRow("L", "i", "null", "", ""),
        InputRow("L", "j", "1", "", ""),
        InputRow("L", "k", "null", "", ""),

        // most frequent values part M
        InputRow("M", "a", "null", "", ""),
        InputRow("M", "b", "0", "", ""),
        InputRow("M", "c", "+inf", "", ""),
        InputRow("M", "d", "1", "", ""),
        InputRow("M", "e", "null", "", ""),
        InputRow("M", "f", "0", "", ""),
        InputRow("M", "g", "-inf", "", ""),
        InputRow("M", "h", "1", "", ""),
        InputRow("M", "i", "4.2", "", ""),
        InputRow("M", "j", "42", "", ""),
        InputRow("M", "k", "null", "", "")
      )
    )
    // part, uid, feature, feature_list, feature_map

  object InputRow {
    import ColumnValueParser._

    def apply(
      part: String,
      uid: String,
      feature: String,
      feature_list: String,
      feature_map: String
    ): _Row =
      _Row(
        part,
        uid,
        feature = parseDouble(feature),
        feature_list = parseArrayDouble(feature_list),
        feature_map = parseMapDouble(feature_map)
      )
    case class _Row(
      part: String,
      uid: String,
      feature: Option[Double],
      feature_list: Option[Array[Option[Double]]],
      feature_map: Option[Map[Option[String], Option[Double]]]
    )
  }

  case class MapTransformer(df: DataFrame, filter: String) {
    import org.apache.spark.sql
    private val spark = df.sparkSession
    import spark.implicits._

    def int_int: DataFrame = map[String, Int, Int, Int](k => k.toInt, v => v)
    def int_double: DataFrame = map[String, Double, Int, Double](k => k.toInt, v => v)
    def int_float: DataFrame = map[String, Float, Int, Float](k => k.toInt, v => v)
    def long_double: DataFrame = map[String, Double, Long, Double](k => k.toLong, v => v)
    def long_float: DataFrame = map[String, Float, Long, Float](k => k.toLong, v => v)
    def long_int: DataFrame = map[String, Int, Long, Int](k => k.toLong, v => v)
    def double_int: DataFrame = map[String, Int, Double, Int](k => k.toDouble, v => v)
    def float_int: DataFrame = map[String, Int, Float, Int](k => k.toFloat, v => v)
    def byte_int: DataFrame = map[String, Int, Byte, Int](k => k.toByte, v => v)
    def short_int: DataFrame = map[String, Int, Short, Int](k => k.toShort, v => v)
    def bool_int: DataFrame =
      map[String, Int, Boolean, Int](k => Try(k.toBoolean).getOrElse(false), v => v)
    def short_byte: DataFrame = map[String, Byte, Short, Byte](k => k.toShort, v => v)
    def byte_byte: DataFrame = map[String, Byte, Byte, Byte](k => k.toByte, v => v)
    def double_long: DataFrame = map[String, Long, Double, Long](k => k.toDouble, v => v)
    def float_long: DataFrame = map[String, Long, Float, Long](k => k.toFloat, v => v)
    def int_short: DataFrame = map[String, Short, Int, Short](k => k.toInt, v => v)
    def long_short: DataFrame = map[String, Short, Long, Short](k => k.toLong, v => v)

    def date_int: DataFrame = map[String, Int, java.sql.Date, Int](
      k => Try(java.sql.Date.valueOf(k)).getOrElse(java.sql.Date.valueOf("2021-05-12")),
      v => v
    )
    def time_int: DataFrame = map[String, Int, java.sql.Timestamp, Int](
      k =>
        Try(java.sql.Timestamp.valueOf(k))
          .getOrElse(java.sql.Timestamp.valueOf("2021-05-12 12:34:55")),
      v => v
    )
    def date_decimal: DataFrame = map[String, sql.types.Decimal, java.sql.Date, sql.types.Decimal](
      k => Try(java.sql.Date.valueOf(k)).getOrElse(java.sql.Date.valueOf("2021-05-12")),
      v => v.toPrecision(4, 3)
    ).selectExpr("part", "uid", "cast(feature as map<date, decimal(4,3)>) as feature")

    def time_decimal: DataFrame =
      map[String, sql.types.Decimal, java.sql.Timestamp, sql.types.Decimal](
        k =>
          Try(java.sql.Timestamp.valueOf(k))
            .getOrElse(java.sql.Timestamp.valueOf("2021-05-12 12:34:55")),
        v => v.toPrecision(4, 3)
      ).selectExpr("part", "uid", "cast(feature as map<timestamp, decimal(4,3)>) as feature")

    def map[IK, IV, OK, OV](
      keyTransform: IK => OK,
      valueTransform: IV => OV
    )(implicit
      ie: Encoder[(String, String, Map[IK, Option[IV]])],
      oe: Encoder[(String, String, Map[OK, Option[OV]])]
    ): DataFrame =
      df.where(filter)
        .as[(String, String, Map[IK, Option[IV]])]
        .map {
          case (part: String, uid: String, feature: Map[IK, Option[IV]]) =>
            val f: Map[OK, Option[OV]] = feature.map {
              case (k, v) => (keyTransform(k), v.map(valueTransform))
            }
            (part, uid, f)
        }
        .toDF("part", "uid", "feature")
  }
}
