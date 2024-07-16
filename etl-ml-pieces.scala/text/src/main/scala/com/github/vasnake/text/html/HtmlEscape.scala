/**
 * Created by vasnake@gmail.com on 2024-07-16
 */
package com.github.vasnake.text.html

import org.unbescape.html.{HtmlEscape => HE}

object HtmlEscape {
  def unescapeHtml(x: String): String = HE.unescapeHtml(x)
}
