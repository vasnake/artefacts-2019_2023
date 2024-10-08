/** Created by vasnake@gmail.com on 2024-07-12
  */
package com.github.vasnake.json

// TODO: do you really need both?
import io.circe._
import org.json4s._

object JsonToolbox {
  def objExpectedErr: Nothing = sys.error("object expected")

  def decodeErr: DecodingFailure => Nothing = (e: DecodingFailure) => sys.error(e.getMessage())

  def j2m(j: Json): Map[String, Json] = j.asObject.getOrElse(objExpectedErr).toMap

  /** parse input text, return json object if it is an object
    * @param text json text
    * @return json object
    */
  def parseJson(text: String): Json = {
    import io.circe._
    import io.circe.parser._

    val parsed = parse(text)
    assert(
      parsed.isRight,
      s"json parser failed: '${parsed.fold(failure => failure.getMessage(), json => "really?")}'"
    )

    parsed.fold(_ => Json.Null, j => j)
  }

  /** find first key in object and return its value
    * @param key key to search
    * @param json json object where key will be found/or not
    * @return value under the first found key
    */
  def getFirstString(key: String, json: Json): Option[String] =
    json
      .findAllByKey(key)
      .head
      .as[String]
      .fold(
        e => None, // sys.error(s"can't find item '${key}' in json: ${e.getMessage}"),
        x => Some(x)
      )

  def readObject[T](
    json: String
  )(implicit
    m: Manifest[T]
  ): T = {
    implicit val formats: DefaultFormats = DefaultFormats

    jackson.JsonMethods.parse(json).extract[T]
  }

  def writeObject[T <: AnyRef](obj: T, format: TextFormatting = JsonToolbox.Compact): String = {
    import jackson.Serialization
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    format match {
      case JsonToolbox.Pretty => Serialization.writePretty(obj)
      case _ => Serialization.write(obj)
    }
  }

  sealed trait TextFormatting
  case object Pretty extends TextFormatting
  case object Compact extends TextFormatting
}
