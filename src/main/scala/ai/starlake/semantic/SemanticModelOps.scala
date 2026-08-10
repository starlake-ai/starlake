package ai.starlake.semantic

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.jdk.CollectionConverters._

/** Format-neutral helpers shared by the semantic model converters (LookML, TMDL). Ossie keeps its
  * own JsonNode helpers untouched.
  */
private[semantic] object SemanticModelOps extends LazyLogging {

  case class ParsedAggregate(aggregate: String, arg: Option[String])

  private val AggregatePattern = """(?is)^(SUM|AVG|MIN|MAX|COUNT)\s*\((.*)\)$""".r
  private val DistinctPattern = """(?is)^DISTINCT\s+(.+)$""".r
  val IdentifierPattern = """^[A-Za-z_][A-Za-z0-9_]*$""".r
  val QualifiedPattern = """^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$""".r

  /** Recognize a whole expression that is exactly one simple aggregate call. Arguments containing
    * parentheses (nested calls, arithmetic) are rejected so callers fall back to raw SQL.
    */
  def parseAggregate(expr: String): Option[ParsedAggregate] =
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

  /** Model-level metrics paired with an ownership flag, keyed by normalized table name. A metric is
    * owned when its qualified aggregate argument names a known table; unowned metrics (flag false)
    * map to the first table so callers render them with their fallback treatment instead of
    * silently mis-scoping them as native measures.
    */
  def assignModelMetrics(
    model: JsonNode,
    tableNames: List[String],
    normalize: String => String
  ): Map[String, List[(JsonNode, Boolean)]] = {
    val normalized = tableNames.map(normalize)
    val metrics = elems(model, "metrics")
    if (normalized.isEmpty) {
      if (metrics.nonEmpty)
        logger.warn("Model-level metrics dropped: the model defines no tables")
      Map.empty
    } else
      metrics
        .map { metric =>
          val owner = for {
            parsed <- parseAggregate(metric.path("expr").asText())
            arg    <- parsed.arg
            table <- arg.trim match {
              case QualifiedPattern(table, _) => Some(normalize(table))
              case _                          => None
            }
            if normalized.contains(table)
          } yield table
          (metric, owner)
        }
        .groupBy { case (_, owner) => owner.getOrElse(normalized.head) }
        .view
        .mapValues(_.map { case (metric, owner) => (metric, owner.isDefined) })
        .toMap
  }

  /** description and synonyms combined into a single text: "<desc>. Synonyms: a, b", or either part
    * alone.
    */
  def combinedDescription(node: JsonNode): Option[String] = {
    val desc = text(node, "description")
    val synonyms = elems(node, "synonyms").map(_.asText()).filter(_.nonEmpty)
    val synPart =
      if (synonyms.nonEmpty) Some(s"Synonyms: ${synonyms.mkString(", ")}") else None
    (desc, synPart) match {
      case (Some(d), Some(s)) => Some(s"$d. $s")
      case (d, s)             => d.orElse(s)
    }
  }

  /** database.schema.table from base_table, falling back to the table name. */
  def baseTableFqn(table: JsonNode): String = {
    val base = table.path("base_table")
    val parts = List("database", "schema", "table").flatMap(k => text(base, k)).filter(_.nonEmpty)
    if (parts.nonEmpty) parts.mkString(".") else table.path("name").asText()
  }

  def elems(node: JsonNode, key: String): List[JsonNode] =
    if (node.has(key) && node.get(key).isArray) node.get(key).elements().asScala.toList
    else Nil

  def text(node: JsonNode, key: String): Option[String] =
    Option(node.path(key).asText(null)).filter(_.nonEmpty)
}
