package ai.starlake.serve.catalog

import scala.util.matching.Regex

/** JDBC drivers and cloud clients sometimes echo credentials in error messages. */
object CatalogScrubber {
  private val SensitiveKey: Regex =
    "(?i).*(password|passwd|pwd|secret|token|private_?key|credential).*".r
  private val UrlSecret: Regex = "(?i)((?:password|passwd|pwd|token|secret|api_?key)=)[^&;\\s]*".r
  private val Mask = "***"

  def scrub(message: String, options: Map[String, String]): String = {
    val secrets = options.toList
      .collect {
        case (key, value) if value != null && value.length >= 3 && SensitiveKey.matches(key) =>
          value
      }
      .sortBy(-_.length)
    val masked = secrets.foldLeft(Option(message).getOrElse("")) { (msg, secret) =>
      msg.replace(secret, Mask)
    }
    UrlSecret.replaceAllIn(masked, m => Regex.quoteReplacement(m.group(1) + Mask))
  }
}
