/**
 * Created by vasnake@gmail.com on 2024-07-16
 */
package com.github.vasnake.spark.udf.`java-api`

import com.github.vasnake.text.html.HtmlEscape.unescapeHtml
import org.apache.spark.sql.api.java.UDF1
import java.lang.{String => jString}


/**
 * html_unescape(string) reproduce https://docs.python.org/3/library/html.html#html.unescape for UTF-8 text
 *
 * sparkSession.udf.registerJavaFunction("html_unescape", "com.github.vasnake.spark.udf.java-api.HtmlUnescapeUDF")
 */
class HtmlUnescapeUDF extends UDF1[jString, jString] {
  override def call(x: jString): jString = {
    if (x == null) null
    else unescapeHtml(x)
  }
}
