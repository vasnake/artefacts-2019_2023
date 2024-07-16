/**
 * Created by vasnake@gmail.com on 2024-07-16
 */
package com.github.vasnake.spark.udf.`java-api`


import org.apache.spark.sql.api.java.UDF1
import java.lang.{Integer => jInteger, String => jString}
import scala.util.hashing.MurmurHash3

/**
  * hash_to_uint32(stringColumn)
  *
  * sparkSession.udf.registerJavaFunction("hash_to_uint32", "com.github.vasnake.spark.udf.`java-api`.HashToUINT32UDF")
  */
class HashToUINT32UDF extends UDF1[String, String] {
  // converting to unsigned not bit-by-bit, but using complement numbers
  // Implementation use string data as a sequence of 16bit chars; two consecutive chars make a 32bit word
  override def call(x: String): String = jInteger.toUnsignedLong(MurmurHash3.stringHash(x)).toString
}

class MurmurHash3_32UDF extends UDF1[jString, jString] {
  // reproduce python func: str(sklearn.utils.murmurhash3_32(bytes, seed=0, positive=True))
  // https://github.com/scikit-learn/scikit-learn/blob/e0ebc7839153da72e091f385ee4e6d4df51f96ef/sklearn/utils/murmurhash.pyx#L80
  override def call(x: jString): jString = {
    if (x == null) null
    else {
      val bytesArray = x.getBytes("UTF-8")

      jInteger.toUnsignedLong(
        MurmurHash3.bytesHash(bytesArray, seed = 0)
      ).toString
    }
  }
}
