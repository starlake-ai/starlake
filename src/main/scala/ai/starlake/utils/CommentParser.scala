package ai.starlake.utils

// From https://users.scala-lang.org/t/solved-parser-combinator-removing-comments/6635
object CommentParser {

  def stripComments(str: String): String = {
    str.replaceAll("(?:/\\*(?:[^*]|(?:\\*+[^*/]))*\\*+/)|(?://.*)", "")
  }
}
