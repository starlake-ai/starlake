package ai.starlake.semantic

import com.typesafe.scalalogging.LazyLogging

/** Converts Snowflake-style semantic models to a LookML project layout: one view file per table
  * plus one model file holding the connection, includes and explores.
  */
object LookMLConverter extends LazyLogging {

  private[semantic] case class ParsedAggregate(lookmlType: String, arg: Option[String])

  private val AggregatePattern = """(?is)^(SUM|AVG|MIN|MAX|COUNT)\s*\((.*)\)$""".r
  private val DistinctPattern = """(?is)^DISTINCT\s+(.+)$""".r

  /** LookML identifiers must match [a-z0-9_]+. Lowercase and replace anything else. */
  private[semantic] def sanitize(name: String): String = {
    val sanitized = name.trim.toLowerCase.replaceAll("[^a-z0-9_]", "_")
    if (sanitized != name)
      logger.warn(s"LookML identifier '$name' sanitized to '$sanitized'")
    sanitized
  }

  /** Recognize a whole expression that is exactly one simple aggregate call. Arguments containing
    * parentheses (nested calls, arithmetic) are rejected so the caller falls back to raw SQL.
    */
  private[semantic] def parseAggregate(expr: String): Option[ParsedAggregate] =
    expr.trim match {
      case AggregatePattern(fn, rawArg) =>
        val arg = rawArg.trim
        if (arg.contains("(") || arg.contains(")")) None
        else
          fn.toUpperCase match {
            case "COUNT" =>
              arg match {
                case "*"                  => Some(ParsedAggregate("count", None))
                case DistinctPattern(col) => Some(ParsedAggregate("count_distinct", Some(col.trim)))
                case _                    => None
              }
            case "SUM" => Some(ParsedAggregate("sum", Some(arg)))
            case "AVG" => Some(ParsedAggregate("average", Some(arg)))
            case "MIN" => Some(ParsedAggregate("min", Some(arg)))
            case "MAX" => Some(ParsedAggregate("max", Some(arg)))
          }
      case _ => None
    }
}
