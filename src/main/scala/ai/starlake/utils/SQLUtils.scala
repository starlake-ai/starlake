package ai.starlake.utils

object SQLUtils {

  /** Syntax parser
    *
    * identifier = X | X.Y.Z | `X` | `X.Y.Z` | `X`.Y.Z
    *
    * FROM identifier
    *
    * FROM parquet.`/path-to/file`
    *
    * FROM
    *
    * JOIN identifier
    */
  //
  def extractRefsFromSQL(sql: String): List[String] = {

    val fromsRegex = "(?i)\\s+FROM\\s+([_\\-a-z0-9`./(]+\\s*[ _,A-Z0-9`./(]*)".r
    val joinRegex = "(?i)\\s+JOIN\\s+([_\\-a-z0-9`./]+)".r
    // val cteRegex = "(?i)\\s+([a-z0-9]+)+\\s+AS\\s*\\(".r

    val froms =
      fromsRegex
        .findAllMatchIn(sql)
        .map(_.group(1))
        .toList
        .flatMap(_.split(",").map(_.trim))
        .map {
          // because the regex above is not powerful enough
          table =>
            val space = table.replaceAll("\n", " ").replace("\t", " ").indexOf(' ')
            if (space > 0)
              table.substring(0, space)
            else
              table
        }
        .filter(!_.contains("(")) // we remove constructions like 'year from date(...)'

    val joins = joinRegex.findAllMatchIn(sql).map(_.group(1)).toList
    // val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList

    (froms ++ joins).map(_.replaceAll("`", ""))
  }

  def extractCTEsFromSQL(sql: String): List[String] = {
    val cteRegex = "(?i)\\s+([a-z0-9]+)+\\s+AS\\s*\\(".r
    val ctes = cteRegex.findAllMatchIn(sql).map(_.group(1)).toList
    ctes.map(_.replaceAll("`", ""))
  }
}
