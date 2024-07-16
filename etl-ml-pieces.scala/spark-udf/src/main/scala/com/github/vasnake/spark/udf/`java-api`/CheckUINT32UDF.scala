/**
 * Created by vasnake@gmail.com on 2024-07-16
 */
package com.github.vasnake.spark.udf.`java-api`

import org.apache.spark.sql.api.java.UDF1

import java.lang.{Boolean => jBoolean}

/**
  * is_uint32(stringColumn)
  *
  * sparkSession.udf.registerJavaFunction("is_uint32", "com.github.vasnake.spark.udf.`java-api`.CheckUINT32UDF")
  */
class CheckUINT32UDF extends UDF1[String, jBoolean] {
  override def call(x: String): jBoolean =
    x.nonEmpty && x.length <= 10 && x.forall(c => c.isDigit) && {
      val longX = x.toLong
      longX >= 0 && longX <= CheckUINT32UDF.MAX_UINT32
    }
}

object CheckUINT32UDF {
  val MAX_UINT32 = 4294967295L // 0xffffffff or math.pow(2, 32) - 1

  import scala.util.Try
  def usingException(x: String): Boolean = {
    Try{ x.toLong }
      .map(longX => longX >= 0 && longX <= CheckUINT32UDF.MAX_UINT32)
      .getOrElse(false)
  }

}
