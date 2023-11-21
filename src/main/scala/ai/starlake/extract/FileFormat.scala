package ai.starlake.extract

import java.nio.charset.StandardCharsets

// Currently handles only CSV configs but may evolve to handle others
case class FileFormat(
  // Common attributes
  encoding: Option[String] = None,
  // CSV
  withHeader: Option[Boolean] = None,
  separator: Option[String] = None,
  // using string instead of char since the following exception is throwing while setting in csv writer settings: java.lang.ClassCastException: class java.lang.String cannot be cast to class java.lang.Character (java.lang.String and java.lang.Character are in module java.base of loader 'bootstrap')
  //	at scala.runtime.BoxesRunTime.unboxToChar(BoxesRunTime.java:91)
  // This behavior is related to the way jackson deserialize JdbcSchemas
  quote: Option[String] = None,
  escape: Option[String] = None,
  nullValue: Option[String] = None,
  datePattern: Option[String] = None,
  timestampPattern: Option[String] = None
) {
  def fillWithDefault(): FileFormat = {
    FileFormat(
      encoding = this.encoding.orElse(Some(StandardCharsets.UTF_8.name())),
      withHeader = this.withHeader.orElse(Some(true)),
      separator = this.separator.orElse(Some(";")),
      quote = this.quote.orElse(Some("\"")),
      escape = this.escape.orElse(Some("\\")),
      datePattern = this.datePattern.orElse(Some("yyyy-MM-dd")),
      timestampPattern = this.timestampPattern.orElse(Some("yyyy-MM-dd HH:mm:ss"))
    )
  }
}
