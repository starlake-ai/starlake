package ai.starlake.semantic

import ai.starlake.config.ConnectionInfo
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer

/** Converts Snowflake-style semantic models to a Power BI TMDL folder: database.tmdl, model.tmdl,
  * relationships.tmdl and one tables/<table>.tmdl per table. Names are kept verbatim and quoted
  * per TMDL rules; indentation uses tabs.
  */
object TMDLConverter extends LazyLogging {

  import SemanticModelOps._

  private val PlainName = """^[A-Za-z0-9_]+$""".r

  private[semantic] case class GeneratedKey(
    table: String,
    columnName: String,
    sourceColumns: List[String]
  )

  /** Convert one semantic model. Returns (relativePath, content) pairs. */
  def convert(
    modelName: String,
    model: JsonNode,
    connection: Option[ConnectionInfo]
  ): Seq[(String, String)] = {
    val tables = elems(model, "tables")
    val metricsByTable =
      assignModelMetrics(model, tables.map(_.path("name").asText()), _.toLowerCase)
    if (model.has("verified_queries"))
      logger.info(s"Model '$modelName': verified_queries have no TMDL equivalent, skipped")

    val files = ArrayBuffer[(String, String)]()
    files += "database.tmdl" -> renderDatabase(modelName)
    files += "model.tmdl" -> renderModelFile(model)
    tables.foreach { table =>
      val name = table.path("name").asText()
      files += s"tables/$name.tmdl" -> renderTable(
        table,
        keys = Nil,
        modelMetrics = metricsByTable.getOrElse(name.toLowerCase, Nil),
        connection = connection
      )
    }
    files.toSeq
  }

  private def renderDatabase(modelName: String): String =
    s"database ${quoteName(modelName)}\n\tcompatibilityLevel: 1600\n"

  private def renderModelFile(model: JsonNode): String = {
    val lines = ArrayBuffer[String]()
    combinedDescription(model).foreach(d => lines += s"/// ${singleLine(d)}")
    lines += "model Model"
    lines += "\tculture: en-US"
    lines.mkString("\n") + "\n"
  }

  private def renderTable(
    table: JsonNode,
    keys: List[GeneratedKey],
    modelMetrics: List[(JsonNode, Boolean)],
    connection: Option[ConnectionInfo]
  ): String = {
    val name = table.path("name").asText()
    val pkColumns = elems(table.path("primary_key"), "columns").map(_.asText())
    val singlePk = if (pkColumns.size == 1) pkColumns.headOption else None
    if (pkColumns.size > 1)
      logger.warn(
        s"Table '$name': composite primary key (${pkColumns.mkString(", ")}) has no TMDL equivalent, skipped"
      )
    if (table.has("filters"))
      logger.info(s"Table '$name': filters have no TMDL equivalent, skipped")

    val lines = ArrayBuffer[String]()
    combinedDescription(table).foreach(d => lines += s"/// ${singleLine(d)}")
    lines += s"table ${quoteName(name)}"
    elems(table, "dimensions").foreach(c => lines ++= column(c, singlePk, isFact = false))
    elems(table, "time_dimensions").foreach(c => lines ++= column(c, singlePk, isFact = false))
    elems(table, "facts").foreach(c => lines ++= column(c, singlePk, isFact = true))
    lines.mkString("\n") + "\n"
  }

  private def column(col: JsonNode, singlePk: Option[String], isFact: Boolean): Seq[String] = {
    val name = col.path("name").asText()
    val dataType = tmdlType(text(col, "data_type"))
    val numeric = Set("int64", "decimal", "double").contains(dataType)
    val lines = ArrayBuffer[String]()
    lines += ""
    combinedDescription(col).foreach(d => lines += s"\t/// ${singleLine(d)}")
    lines += s"\tcolumn ${quoteName(name)}"
    lines += s"\t\tdataType: $dataType"
    if (singlePk.contains(name)) lines += "\t\tisKey"
    lines += s"\t\tsourceColumn: $name"
    lines += s"\t\tsummarizeBy: ${if (isFact && numeric) "sum" else "none"}"
    lines.toSeq
  }

  // ── helpers ──────────────────────────────────────────────────────────

  /** TMDL names are kept verbatim; quote when not a plain [A-Za-z0-9_]+ identifier. */
  private def quoteName(name: String): String =
    if (PlainName.matches(name)) name else s"'${name.replace("'", "''")}'"

  private def singleLine(s: String): String = s.replaceAll("\\s+", " ").trim

  /** Map semantic data_type to the TMDL dataType enum; unknown or absent types become string. */
  private def tmdlType(raw: Option[String]): String =
    raw.map(_.trim.toUpperCase).getOrElse("") match {
      case "INT" | "INTEGER" | "BIGINT" | "SMALLINT"      => "int64"
      case "NUMBER" | "NUMERIC" | "DECIMAL"               => "decimal"
      case "FLOAT" | "DOUBLE" | "REAL"                    => "double"
      case "TEXT" | "STRING" | "VARCHAR" | "CHAR"         => "string"
      case "BOOLEAN" | "BOOL"                             => "boolean"
      case "DATE" | "DATETIME" | "TIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" |
          "TIMESTAMP_TZ" =>
        "dateTime"
      case _ => "string"
    }
}
