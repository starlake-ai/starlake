package ai.starlake.utils

import ai.starlake.config.Settings

import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
object Formatter extends Formatter

trait Formatter {

  /** Split a String into a Map
    * @param str
    *   : the string to be splitted
    * @return
    */
  implicit class RichFormatter(str: String) {
    def richFormat(
      activeEnv: Map[String, Any],
      extraEnvVars: Map[String, Any]
    )(implicit settings: Settings): String = {
      if (settings.appConfig.internal.forall(_.substituteVars))
        (activeEnv ++ extraEnvVars).foldLeft(str) { case (res, (key, value)) =>
          res
            .replaceAll(
              settings.appConfig.sqlParameterPattern.format(key),
              Regex.quoteReplacement(value.toString)
            )
            .replaceAll(
              "\\{\\{\\s*%s\\s*\\}\\}".format(key),
              Regex.quoteReplacement(value.toString)
            )
        }
      else
        str
    }

    def extractVars()(implicit settings: Settings): Set[String] = {
      val pattern = Pattern.compile("\\{\\{\\s*([a-zA-Z_0-9]+)\\s*}}").matcher(str)

      val result = ListBuffer[String]()
      while (pattern.find())
        result.append(pattern.group(1))

      result.toSet
    }

    def splitSql(separator: String = ";\n"): List[String] = {
      str.split(separator).filter(_.trim.nonEmpty).toList
    }
  }
}
