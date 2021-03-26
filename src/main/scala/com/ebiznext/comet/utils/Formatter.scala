package com.ebiznext.comet.utils

object Formatter extends Formatter

trait Formatter {

  /** Split a String into a Map
    * @param str : the string to be splitted
    * @return
    */
  implicit class RichFormatter(str: String) {

    def richFormat(replacement: Map[String, String]): String =
      replacement.foldLeft(str) { (res, entry) =>
        val key = entry._1
        val value = entry._2
        res.replaceAll("\\{\\{%s\\}\\}".format(key), value)
      }
  }
}
