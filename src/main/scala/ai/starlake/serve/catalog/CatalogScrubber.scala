package ai.starlake.serve.catalog

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

/** JDBC drivers and cloud clients sometimes echo credentials in error messages. */
object CatalogScrubber {
  private val SensitiveKey: Regex =
    "(?i).*(password|passwd|pwd|secret|token|private_?key|credential).*".r
  private val UrlSecret: Regex = "(?i)((?:password|passwd|pwd|token|secret|api_?key)=)[^&;\\s]*".r
  private val Mask = "***"

  /** Connection options safe to display: sorted by key, secrets masked. */
  def maskOptions(options: Map[String, String]): ListMap[String, String] =
    ListMap.from(options.toList.sortBy(_._1).map { case (key, value) =>
      val shown =
        if (SensitiveKey.matches(key)) Mask
        else if (value == null) ""
        else UrlSecret.replaceAllIn(value, m => Regex.quoteReplacement(m.group(1) + Mask))
      key -> shown
    })

  def scrub(message: String, options: Iterable[(String, String)]): String = {
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
