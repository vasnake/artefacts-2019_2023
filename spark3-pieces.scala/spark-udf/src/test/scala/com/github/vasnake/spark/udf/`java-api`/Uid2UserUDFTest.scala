/** Created by vasnake@gmail.com on 2024-08-13
  */
package com.github.vasnake.spark.udf.`java-api`

//import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

class Uid2UserUDFTest extends AnyFlatSpec with should.Matchers {
  val udf = new Uid2UserUDF()

  private def checkValid(uid_type: String, data: Seq[(String, String)]): Unit =
    data.foreach { case (uid, expected) => assert(udf.call(uid, uid_type) === expected) }

  private def checkInvalid(uid_type: String, data: Seq[String]): Unit =
    data.foreach(uid => assert(udf.call(uid, uid_type) === null))

  it should "pass valid UID" in {
    // testOnly *Uid2UserUDF* -- -z " valid UID"
    checkValid(
      "UID",
      Seq(
        ("1234567890", "uid:1234567890"),
        ("-0", "uid:-0"),
        ("9223372036854775807", "uid:9223372036854775807"),
        ("-9223372036854775808", "uid:-9223372036854775808")
      )
    )
  }

  it should "pass valid T1ID" in {
    // testOnly *Uid2UserUDF* -- -z " valid T1ID"
    checkValid(
      "T1ID",
      Seq(
        ("1234567890", "t1id:1234567890"),
        ("-0", "t1id:-0"),
        ("9223372036854775807", "t1id:9223372036854775807"),
        ("-9223372036854775808", "t1id:-9223372036854775808")
      )
    )
  }

  it should "pass valid T2ID" in {
    // testOnly *Uid2UserUDF* -- -z " valid T2ID"
    checkValid(
      "T2ID",
      Seq(
        ("1234567890", "t2id:1234567890"),
        ("0", "t2id:0"),
        ("9223372036854775807", "t2id:9223372036854775807"),
        ("-9223372036854775808", "t2id:-9223372036854775808")
      )
    )
  }

  it should "pass valid T3ID" in {
    // testOnly *Uid2UserUDF* -- -z " valid T3ID"
    checkValid(
      "T3ID",
      Seq(
        ("9A8F7B", "t3id:9A8F7B"),
        ("7FFFFFFFFFFFFFFF", "t3id:7FFFFFFFFFFFFFFF"),
        ("-7aaaaaaaaaaaaaaa", "t3id:-7aaaaaaaaaaaaaaa"),
        ("0", "t3id:0")
      )
    )
  }

  it should "return valid GAID" in {
    // testOnly *Uid2UserUDF* -- -z " valid GAID"
    checkValid(
      "GAID",
      Seq(
        ("B5CD47AA8F6B4534A0675C8C21EFD375", "gaid:b5cd47aa-8f6b-4534-a067-5c8c21efd375"),
        ("00000000000000000000000000000000", "gaid:00000000-0000-0000-0000-000000000000")
      )
    )
  }

  it should "return valid IDFA" in {
    // testOnly *Uid2UserUDF* -- -z " valid IDFA"
    checkValid(
      "IDFA",
      Seq(
        ("B5CD47AA8F6B4534A0675C8C21EFD375", "idfa:B5CD47AA-8F6B-4534-A067-5C8C21EFD375"),
        ("ffffffffffffffffffffffffffffffff", "idfa:FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")
      )
    )
  }

  it should "pass valid EMAIL" in {
    // testOnly *Uid2UserUDF* -- -z " valid EMAIL"
    checkValid(
      "EMAIL",
      Seq(
        ("foo@bar.baz", "email:foo@bar.baz"),
        ("a.@b. .c", "email:a.@b. .c"),
        (" a . @ b . . c ", "email: a . @ b . . c "),
        (" @ . ", "email: @ . ")
      )
    )
  }

  it should "pass valid MAIL" in {
    // testOnly *Uid2UserUDF* -- -z " valid MAIL"
    checkValid(
      "MAIL",
      Seq(
        ("foo@bar.baz", "mail:foo@bar.baz"),
        ("a@b.c", "mail:a@b.c"),
        ("a.@b..c", "mail:a.@b..c")
      )
    )
  }

  it should "return null on invalid UID" in {
    // testOnly *Uid2UserUDF* -- -z "invalid UID"
    checkInvalid(
      "UID",
      Seq(
        "123BEEF",
        "-",
        "-9223372036854775809"
      )
    )
  }

  it should "return null on invalid T1ID" in {
    checkInvalid(
      "T1ID",
      Seq(
        "123BEEF",
        "9223372036854775808",
        "+"
      )
    )
  }

  it should "return null on invalid T2ID" in {
    checkInvalid(
      "T2ID",
      Seq(
        "123BEEF",
        "",
        "+",
        "-"
      )
    )
  }

  it should "return null on invalid T3ID" in {
    checkInvalid(
      "T3ID",
      Seq(
        "123XYZ",
        "",
        "-",
        "FFFFFFFFFFFFFFFF1"
      )
    )
  }

  it should "return null on invalid GAID" in {
    // testOnly *Uid2UserUDF* -- -z "invalid GAID"
    checkInvalid(
      "GAID",
      Seq(
        "123XYZ",
        "Z5CD47AA8F6B4534A0675C8C21EFD375",
        "123456789abcdef9876543210FEDCBAG",
        "B5CD47AA-8F6B-4534-A067-5C8C21EFD375"
      )
    )
  }

  it should "return null on invalid IDFA" in {
    // testOnly *Uid2UserUDF* -- -z "invalid IDFA"
    checkInvalid(
      "IDFA",
      Seq(
        "123XYZ",
        "B5CD47AA8F6B4534A0675C8C21EFD3750",
        "0123456789abcdef9876543210FEDCBA-"
      )
    )
  }

  it should "return null on invalid EMAIL" in {
    // testOnly *Uid2UserUDF* -- -z "invalid EMAIL"
    checkInvalid(
      "EMAIL",
      Seq(
        ".@.",
        "@",
        "@.",
        "..",
        "a.b.c"
      )
    )
  }

  it should "return null on invalid MAIL" in {
    // testOnly *Uid2UserUDF* -- -z "invalid MAIL"
    checkInvalid(
      "MAIL",
      Seq(
        "a@b",
        "",
        ".@.",
        "a@.b.",
        "a@.b",
        "a@b"
      )
    )
  }
}
