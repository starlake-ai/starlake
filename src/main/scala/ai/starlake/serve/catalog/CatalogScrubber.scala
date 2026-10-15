package ai.starlake.serve.catalog

import scala.collection.immutable.ListMap
import scala.util.matching.Regex

/** JDBC drivers and cloud clients sometimes echo credentials in error messages. */
object CatalogScrubber {
  private val Words =
    "password|passwd|pwd|secret|token|private_?key|credential|api_?key|access_?key|" +
    "account_?key|sas_?key|json_?key|key_?base64|key_?file"
  private val SensitiveKey: Regex = s"(?i).*($Words).*".r
  private val UrlSecret: Regex =
    s"(?i)((?:password|passwd|pwd|token|secret|private_?key|credential|api_?key|access_?key|account_?key|sas_?key)=)[^&;\\s]*".r
  private val UrlUserInfo: Regex = "(?i)(://[^/:@\\s]+:)[^@/\\s]+(@)".r
  private val Mask = "***"

  private def maskUrl(text: String): String = {
    val params =
      UrlSecret.replaceAllIn(text, m => Regex.quoteReplacement(m.group(1) + Mask))
    UrlUserInfo.replaceAllIn(
      params,
      m => Regex.quoteReplacement(m.group(1) + Mask + m.group(2))
    )
  }

  /** Connection options safe to display: sorted by key, secrets masked. */
  def maskOptions(options: Map[String, String]): ListMap[String, String] =
    ListMap.from(options.toList.sortBy(_._1).map { case (key, value) =>
      val shown =
        if (SensitiveKey.matches(key)) Mask
        else if (value == null) ""
        else maskUrl(value)
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
    maskUrl(masked)
  }
}
