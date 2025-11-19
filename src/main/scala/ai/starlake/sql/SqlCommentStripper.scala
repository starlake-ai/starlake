package ai.starlake.sql

object SqlCommentStripper {

  /** Removes single-line (--) and multi-line (/* ... */) comments from a SQL string. It correctly
    * handles comments within single-quoted string literals.
    *
    * @param sql
    *   The input SQL string.
    * @return
    *   The SQL string with all comments removed.
    */
  def stripComments(sql: String): String = {
    if (sql == null || sql.isEmpty) return sql

    val result = new StringBuilder()
    val length = sql.length
    var i = 0

    // State variables (mutable 'var' in Scala)
    var inQuote = false // true if inside a single-quoted string literal ('...')
    var inLineComment = false // true if inside a single-line comment (--)
    var inBlockComment = false // true if inside a multi-line comment (/* ... */)

    while (i < length) {
      val currentChar = sql.charAt(i)

      // --- State: In a Multi-Line Comment ---
      if (inBlockComment) {
        // Look for the end of the block comment: */
        if (currentChar == '*' && i + 1 < length && sql.charAt(i + 1) == '/') {
          inBlockComment = false
          i += 2 // Skip '*' and '/'
        } else {
          i += 1 // Skip current character
        }

        // --- State: In a Single-Line Comment ---
      } else if (inLineComment) {
        // Look for the end of the line comment (newline or end of string)
        if (currentChar == '\n' || currentChar == '\r') {
          inLineComment = false
          result.append(currentChar) // Keep the newline for formatting
        }
        i += 1 // Skip current character

        // --- State: In a String Literal ---
      } else if (inQuote) {
        result.append(currentChar)
        // Look for an escaped quote '' or the closing quote '
        if (currentChar == '\'') {
          if (i + 1 < length && sql.charAt(i + 1) == '\'') {
            result.append('\'') // Append the escaped quote
            i += 2 // Skip both 's
          } else {
            inQuote = false // Closing quote
            i += 1
          }
        } else {
          i += 1
        }

        // --- State: Normal Code (Not in any comment or quote) ---
      } else {

        // Check for Start of Multi-Line Comment: /*
        if (currentChar == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
          inBlockComment = true
          i += 2 // Skip '/' and '*'

          // Check for Start of Single-Line Comment: --
        } else if (currentChar == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
          inLineComment = true
          i += 2 // Skip both '-'s

          // Check for Start of String Literal: '
        } else if (currentChar == '\'') {
          inQuote = true
          result.append(currentChar)
          i += 1

          // Normal Character
        } else {
          result.append(currentChar)
          i += 1
        }
      }
    }

    removeEmptyLines(result.toString())
  }

  def removeEmptyLines(input: String): String = {
    val res = input.linesIterator.filter(_.trim.nonEmpty).mkString("\n")
    // remove last ';' if any
    if (res.trim.endsWith(";")) res.trim.dropRight(1) else res
  }
}
