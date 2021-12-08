package ai.starlake.utils

object ViewExtractor {
  val viewPattern = "ref\\((\\s*[0-9a-zA-Z_]*\\s*)\\)".r

  def parse(sqlContent: String): (String, List[String]) = {
    var result = sqlContent
    val patterns = viewPattern
      .findAllMatchIn(sqlContent)
      .map { patternMatch =>
        patternMatch.group(1)
      }
      .toList

    patterns
      .foreach { pattern =>
        val ref = s"ref\\($pattern\\)"
        result = result.replaceAll(ref, pattern.trim)
      }
    (result, patterns.map(_.trim))
  }
}
