package ai.starlake.utils

import scala.util.parsing.combinator.JavaTokenParsers

// From https://users.scala-lang.org/t/solved-parser-combinator-removing-comments/6635
object CommentParser extends JavaTokenParsers {

  def singleLine: Parser[String] = "//.*".r ^^ (_ => "")
  def multiLine: Parser[String] = """/\*.*\*/""".r ^^^ ""
  def comments: Parser[Seq[String]] = (singleLine | multiLine).*
  def commentedText: Parser[String] = comments ~> "[^\\/*]*".r <~ comments
  def empty: Parser[Seq[String]] = ".*$".r ^^ { e => Seq(e) }
  def expression: Parser[String] = commentedText ~ (empty | commentedText.*) ^^ {
    case (a: String) ~ (b: Seq[String]) => a + b.mkString("")
  }

  def stripComments(str: String): Either[String, String] = {
    parseAll(expression, str) match {
      case Success(result, _) => Right(result)
      case failedOrIncomplete => Left(failedOrIncomplete.toString)
    }
  }
}
