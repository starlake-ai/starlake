package ai.starlake.utils

import ai.starlake.config.Settings
object Formatter extends Formatter

trait Formatter {

  /** Split a String into a Map
    * @param str
    *   : the string to be splitted
    * @return
    */
  implicit class RichFormatter(str: String) {
    def richFormat(
      activeEnv: Map[String, String],
      extraEnvVars: Map[String, String]
    )(implicit settings: Settings): String = {
      if (settings.comet.internal.forall(_.substituteVars))
        (activeEnv ++ extraEnvVars).foldLeft(str) { case (res, (key, value)) =>
          res
            .replaceAll(settings.comet.sqlParameterPattern.format(key), value) // new syntax
            .replaceAll("\\{\\{\\s*%s\\s*\\}\\}".format(key), value) // old syntax
        }
      else
        str
    }
  }
}
