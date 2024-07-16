
/**
 * Created by vasnake@gmail.com on 2024-07-16
 */
package com.github.vasnake.spark.udf.`java-api`

import org.apache.spark.sql.api.java.UDF2
import java.lang.{String => jString}
import scala.util.Try

/**
  * uid2user(uid, uid_type)
  *
  * sparkSession.udf.registerJavaFunction("uid2user", "com.github.vasnake.spark.udf.`java-api`.Uid2UserUDF")
  */
class Uid2UserUDF extends UDF2[jString, jString, jString] {
  override def call(uid: jString, uid_type: jString): jString =
    if (uid == null || uid_type == null) null
    else Uid2UserUDFImpl.uid2user(uid, uid_type).orNull
}

object Uid2UserUDFImpl {
  //UID: число в домене int64 в виде строки base10 (e.g. 1234567890) => uid:1234567890
  //T1ID: число в домене int64 в виде строки base10 (e.g. 1234567890) => t1id:1234567890
  //T2ID: число в домене int64 в виде строки base10 (e.g. 1234567890) => t2id:1234567890
  //T3ID: число в домене int64 в виде строки base16 (e.g. 9A8F7B) => t3id:9A8F7B
  //GAID: число в домене uint128 в виде строки base16 (e.g. B5CD47AA8F6B4534A0675C8C21EFD375) => gaid:b5cd47aa-8f6b-4534-a067-5c8c21efd375
  //IDFA: число в домене uint128 в виде строки base16 (e.g. B5CD47AA8F6B4534A0675C8C21EFD375) => idfa:B5CD47AA-8F6B-4534-A067-5C8C21EFD375
  //EMAIL: строка адреса email (e.g. foo@bar.baz) => email:foo@bar.baz
  //MAIL: строка адреса email (e.g. foo@bar.baz) => mail:foo@bar.baz

  // TODO: consider boosting performance by eliminating map, option, exception
  def uid2user(uid: String, uid_type: String): Option[String] =
    for {
      prefix <- uidTypeMapping.get(uid_type)
      user <- Try(uidMapping(prefix).apply(uid)).toOption
    } yield s"${prefix}:${user}"

  private val uidTypeMapping: Map[String, String] = Map(
    "UID" -> "uid",
    "T1ID" -> "t1id",
    "T2ID" -> "t2id",
    "T3ID" -> "t3id",
    "GAID" -> "gaid",
    "IDFA" -> "idfa",
    "EMAIL" -> "email",
    "MAIL" -> "mail"
  )

  private val uidMapping: Map[String, String => String] = Map(
    "uid" -> {uid: String => int64Base10(uid)},
    "t1id" -> {uid: String => int64Base10(uid)},
    "t2id" -> {uid: String => int64Base10(uid)},
    "t3id" -> {uid: String => int64Base16(uid)},
    "gaid" -> {uid: String => canonicalUUID(uid.toLowerCase)},
    "idfa" -> {uid: String => canonicalUUID(uid.toUpperCase)},
    "email" -> {uid: String => email(uid)},
    "mail" -> {uid: String => email(uid)}
  ).withDefault(_ => identity)

  // Validate and transform. If value is invalid throw an exception

  private def int64Base10(num: String): String = {
    java.lang.Long.parseLong(num, 10)

    num
  }

  private def int64Base16(num: String): String = {
    java.lang.Long.parseLong(num, 16)

    num
  }

  private def email(addr: String): String = {
    // ^[^@]+@[^@]+\.[^@]+$
    // only one `@` not on first position
    // after `@` and at least 1 symbol there is `.`
    // after `.` at least one symbol

    var char: Char = '@'
    var ampIdx: Int = -1
    var dotIdx: Int = -1

    var i: Int = 0
    val len: Int = addr.length

    require(len >= 5, s"Invalid email, wrong length, `${addr}`")

    while (i < len) {
      char = addr(i)

      if (char == '@') {
        require(i > 0 && ampIdx < 0, s"Invalid email, wrong ampersand position, `${addr}`")
        ampIdx = i
      }

      if (char == '.' && ampIdx > 0) {
        require((i - ampIdx) > 1, s"Invalid email, wrong dot position, `${addr}`")
        dotIdx = i
      }

      i += 1
    }

    require(ampIdx > 0, s"Invalid email, no ampersand, `${addr}`")
    require(dotIdx > (ampIdx + 1), s"Invalid email, no dot, `${addr}`")
    require((len - dotIdx) > 1, s"Invalid email, dot in last position, `${addr}`")

    addr
  }

  private def canonicalUUID(compact: String): String = {
    require(compact.length == 32, s"UUID string length != 32, `${compact}`")

    require(
      compact.forall(c => (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9')),
      s"Invalid chars in UUID string, `${compact}`"
    )

    s"${compact.substring(0, 8)}-${compact.substring(8, 12)}-${compact.substring(12, 16)}-${compact.substring(16, 20)}-${compact.substring(20, 32)}"
  }

}
