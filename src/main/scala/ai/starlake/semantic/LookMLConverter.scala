package ai.starlake.semantic

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/** Converts Snowflake-style semantic models to a LookML project layout: one view file per table
  * plus one model file holding the connection, includes and explores.
  */
object LookMLConverter extends LazyLogging {

  private[semantic] case class ParsedAggregate(lookmlType: String, arg: Option[String])

  private val AggregatePattern = """(?is)^(SUM|AVG|MIN|MAX|COUNT)\s*\((.*)\)$""".r
  private val DistinctPattern = """(?is)^DISTINCT\s+(.+)$""".r
  private val IdentifierPattern = """^[A-Za-z_][A-Za-z0-9_]*$""".r
  private val QualifiedPattern = """^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$""".r

  /** Convert one semantic model. Returns (relativePath, content) pairs: the model file first, then
    * one view file per table.
    */
  def convert(modelName: String, model: JsonNode, connection: String): Seq[(String, String)] = {
    val tables = elems(model, "tables")
    val metricsByView = assignModelMetrics(model, tables)
    val views = tables.map { table =>
      val viewName = sanitize(table.path("name").asText())
      s"$viewName.view.lkml" -> renderView(table, metricsByView.getOrElse(viewName, Nil))
    }
    if (model.has("verified_queries"))
      logger.info(s"Model '$modelName': verified_queries have no LookML equivalent, skipped")
    (s"${sanitize(modelName)}.model.lkml" -> renderModel(model, connection)) +: views
  }

  /** Model-level metrics are attached to the view owning the aggregate argument (COUNT(DISTINCT
    * customers.customer_id) lands in the customers view); metrics whose owning table cannot be
    * determined go to the first view.
    */
  private def assignModelMetrics(
    model: JsonNode,
    tables: List[JsonNode]
  ): Map[String, List[JsonNode]] = {
    val viewNames = tables.map(t => sanitize(t.path("name").asText()))
    val metrics = elems(model, "metrics")
    if (viewNames.isEmpty) {
      if (metrics.nonEmpty)
        logger.warn("Model-level metrics dropped: the model defines no tables")
      Map.empty
    } else
      metrics.groupBy { metric =>
        val owner = for {
          parsed <- parseAggregate(metric.path("expr").asText())
          arg    <- parsed.arg
          table <- arg.trim match {
            case QualifiedPattern(table, _) => Some(sanitize(table))
            case _                          => None
          }
          if viewNames.contains(table)
        } yield table
        owner.getOrElse(viewNames.head)
      }
  }

  private def renderView(table: JsonNode, modelMetrics: List[JsonNode]): String = {
    val viewName = sanitize(table.path("name").asText())
    val pkColumns = elems(table.path("primary_key"), "columns").map(c => sanitize(c.asText()))
    val singlePk = if (pkColumns.size == 1) pkColumns.headOption else None
    val fieldNames =
      (elems(table, "dimensions") ++ elems(table, "time_dimensions") ++ elems(table, "facts"))
        .map(f => sanitize(f.path("name").asText()))

    val lines = ArrayBuffer[String]()
    lines += s"view: $viewName {"
    lines += s"  sql_table_name: ${source(table)} ;;"
    if (pkColumns.size > 1)
      lines += s"  # composite primary key (${pkColumns.mkString(", ")}) not representable in LookML"

    elems(table, "dimensions").foreach { d =>
      lines += ""
      lines ++= dimension(d, singlePk, forcedNumber = false)
    }
    elems(table, "time_dimensions").foreach { d =>
      lines += ""
      lines ++= dimensionGroup(d)
    }
    elems(table, "facts").foreach { f =>
      lines += ""
      lines ++= dimension(f, singlePk, forcedNumber = true)
    }
    (elems(table, "metrics") ++ modelMetrics).foreach { m =>
      lines += ""
      lines ++= measure(m, viewName, fieldNames)
    }

    val filters = elems(table, "filters")
    if (filters.nonEmpty) {
      lines += ""
      lines += "  # filters (not translated to LookML):"
      filters.foreach { f =>
        lines += s"  #   ${f.path("name").asText()}: ${f.path("expr").asText()}"
      }
    }
    lines += "}"
    lines.mkString("\n") + "\n"
  }

  private def dimension(
    col: JsonNode,
    singlePk: Option[String],
    forcedNumber: Boolean
  ): Seq[String] = {
    val name = sanitize(col.path("name").asText())
    val sql = text(col, "expr").getOrElse(s"$${TABLE}.$name")
    val lookmlType = text(col, "data_type")
      .flatMap(dimensionType)
      .orElse(if (forcedNumber) Some("number") else None)
    val lines = ArrayBuffer[String]()
    lines += s"  dimension: $name {"
    if (singlePk.contains(name)) lines += "    primary_key: yes"
    lookmlType.foreach(t => lines += s"    type: $t")
    lines += s"    sql: $sql ;;"
    description(col).foreach(d => lines += s"    description: ${quote(d)}")
    val samples = elems(col, "sample_values").map(_.asText())
    if (samples.nonEmpty)
      lines += s"    suggestions: [${samples.map(quote).mkString(", ")}]"
    lines += "  }"
    lines.toSeq
  }

  private def dimensionGroup(col: JsonNode): Seq[String] = {
    val name = sanitize(col.path("name").asText())
    val sql = text(col, "expr").getOrElse(s"$${TABLE}.$name")
    val lines = ArrayBuffer[String]()
    lines += s"  dimension_group: $name {"
    lines += "    type: time"
    lines += "    timeframes: [raw, date, week, month, quarter, year]"
    if (text(col, "data_type").exists(_.trim.equalsIgnoreCase("DATE")))
      lines += "    datatype: date"
    lines += s"    sql: $sql ;;"
    description(col).foreach(d => lines += s"    description: ${quote(d)}")
    lines += "  }"
    lines.toSeq
  }

  private def measure(m: JsonNode, viewName: String, fieldNames: List[String]): Seq[String] = {
    val name = sanitize(m.path("name").asText())
    val expr = m.path("expr").asText()
    val lines = ArrayBuffer[String]()
    parseAggregate(expr) match {
      case Some(ParsedAggregate(lookmlType, arg)) =>
        lines += s"  measure: $name {"
        lines += s"    type: $lookmlType"
        arg.foreach { raw =>
          // Strip a qualifier naming this view, then substitute ${field} for known fields.
          val local = raw.trim match {
            case QualifiedPattern(table, column) if sanitize(table) == viewName => column
            case other                                                          => other
          }
          val sql =
            if (IdentifierPattern.matches(local) && fieldNames.contains(sanitize(local)))
              s"$${${sanitize(local)}}"
            else local
          lines += s"    sql: $sql ;;"
        }
      case None =>
        lines += "  # starlake: verify this measure, expression could not be mapped to a native LookML type"
        lines += s"  measure: $name {"
        lines += "    type: number"
        lines += s"    sql: $expr ;;"
    }
    description(m).foreach(d => lines += s"    description: ${quote(d)}")
    lines += "  }"
    lines.toSeq
  }

  private def renderModel(model: JsonNode, connection: String): String = {
    val tables = elems(model, "tables").map(t => sanitize(t.path("name").asText()))
    val relationships = elems(model, "relationships")

    val lines = ArrayBuffer[String]()
    lines += s"connection: ${quote(connection)}"
    lines += "include: \"*.view.lkml\""

    val byLeftTable = relationships.groupBy(r => sanitize(r.path("left_table").asText()))
    relationships.map(r => sanitize(r.path("left_table").asText())).distinct.foreach { left =>
      lines += ""
      lines += s"explore: $left {"
      byLeftTable(left).foreach { rel =>
        val right = sanitize(rel.path("right_table").asText())
        val joinType = text(rel, "join_type").getOrElse("left_outer")
        val relationshipType = text(rel, "relationship_type").getOrElse("many_to_one")
        val sqlOn = elems(rel, "relationship_columns")
          .flatMap { rc =>
            for {
              l <- text(rc, "left_column")
              r <- text(rc, "right_column")
            } yield s"$${$left.${sanitize(l)}} = $${$right.${sanitize(r)}}"
          }
          .mkString(" AND ")
        lines += s"  join: $right {"
        lines += s"    type: $joinType"
        lines += s"    relationship: $relationshipType"
        lines += s"    sql_on: $sqlOn ;;"
        lines += "  }"
      }
      lines += "}"
    }

    val inRelationship = relationships.flatMap { r =>
      List(sanitize(r.path("left_table").asText()), sanitize(r.path("right_table").asText()))
    }.toSet
    tables.filterNot(inRelationship).foreach { t =>
      lines += ""
      lines += s"explore: $t {}"
    }
    lines.mkString("\n") + "\n"
  }

  // ── helpers ──────────────────────────────────────────────────────────

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

  private def description(node: JsonNode): Option[String] = {
    val desc = text(node, "description")
    val synonyms = elems(node, "synonyms").map(_.asText()).filter(_.nonEmpty)
    val synPart =
      if (synonyms.nonEmpty) Some(s"Synonyms: ${synonyms.mkString(", ")}") else None
    (desc, synPart) match {
      case (Some(d), Some(s)) => Some(s"$d. $s")
      case (d, s)             => d.orElse(s)
    }
  }

  private def dimensionType(raw: String): Option[String] =
    raw.trim.toUpperCase match {
      case "NUMBER" | "NUMERIC" | "DECIMAL" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "FLOAT" |
          "DOUBLE" | "REAL" =>
        Some("number")
      case "TEXT" | "STRING" | "VARCHAR" | "CHAR" => Some("string")
      case "BOOLEAN" | "BOOL"                     => Some("yesno")
      case _                                      => None
    }

  private def source(table: JsonNode): String = {
    val base = table.path("base_table")
    val parts = List("database", "schema", "table").flatMap(k => text(base, k)).filter(_.nonEmpty)
    if (parts.nonEmpty) parts.mkString(".") else table.path("name").asText()
  }

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def elems(node: JsonNode, key: String): List[JsonNode] =
    if (node.has(key) && node.get(key).isArray) node.get(key).elements().asScala.toList
    else Nil

  private def text(node: JsonNode, key: String): Option[String] =
    Option(node.path(key).asText(null)).filter(_.nonEmpty)
}
