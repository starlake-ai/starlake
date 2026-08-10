package ai.starlake.semantic

import ai.starlake.config.ConnectionInfo
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer

/** Converts Snowflake-style semantic models to a Power BI TMDL folder: database.tmdl, model.tmdl,
  * relationships.tmdl and one tables/<table>.tmdl per table. Names are kept verbatim and quoted per
  * TMDL rules; indentation uses tabs.
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
    files += "model.tmdl"    -> renderModelFile(model)
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
    lines ++= partition(table, connection)
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

  private def partition(table: JsonNode, connection: Option[ConnectionInfo]): Seq[String] = {
    val name = table.path("name").asText()
    val src = PbiSource.resolve(connection, table)
    val lines = ArrayBuffer[String]()
    lines += ""
    lines += s"\tpartition ${quoteName(name)} = m"
    lines += "\t\tmode: import"
    lines += "\t\tsource ="
    lines += "\t\t\tlet"
    src.lines.foreach(l => lines += s"\t\t\t\t$l")
    lines += s"\t\t\t\tResult = Value.NativeQuery(${src.queryTarget}, ${mString(nativeQuery(table))})"
    lines += "\t\t\tin"
    lines += "\t\t\t\tResult"
    lines.toSeq
  }

  /** SELECT <expr> AS <name>, ... FROM <fqn>; a field whose expr equals (or defaults to) its name
    * is projected bare. SELECT * when the table has no fields.
    */
  private def nativeQuery(table: JsonNode): String = {
    val fields =
      elems(table, "dimensions") ++ elems(table, "time_dimensions") ++ elems(table, "facts")
    val fqn = baseTableFqn(table)
    if (fields.isEmpty) s"SELECT * FROM $fqn"
    else {
      val cols = fields.map { f =>
        val n = f.path("name").asText()
        val e = text(f, "expr").getOrElse(n)
        if (e == n) n else s"$e AS $n"
      }
      s"SELECT ${cols.mkString(", ")} FROM $fqn"
    }
  }

  /** M string literal: double quotes escape by doubling. */
  private def mString(s: String): String = "\"" + s.replace("\"", "\"\"") + "\""

  private[semantic] case class MSource(lines: Seq[String], queryTarget: String)

  /** Maps a Starlake connection to Power Query (M) source lines. Engine detection replicates
    * ConnectionInfo.getJdbcEngineName's URL-scheme logic without constructing an Engine, so
    * unmapped names degrade to the generic fallback instead of failing.
    */
  private[semantic] object PbiSource {

    private val UrlPattern = """jdbc:[A-Za-z0-9]+://([^:/?]+)(?::(\d+))?(?:/([^?]+))?.*""".r

    def resolve(connection: Option[ConnectionInfo], table: JsonNode): MSource =
      connection match {
        case None => fallback
        case Some(info) =>
          engineOf(info) match {
            case "bigquery" =>
              val project = info.options
                .get("gcpProjectId")
                .orElse(text(table.path("base_table"), "database"))
                .getOrElse("PROJECT_TODO")
              MSource(
                Seq(s"Source = GoogleBigQuery.Database([BillingProject=${mString(project)}]),"),
                "Source"
              )
            case "snowflake" =>
              val host = hostOf(info).getOrElse("SERVER_TODO")
              val warehouse = info.options
                .get("warehouse")
                .orElse(info.options.get("sfWarehouse"))
                .getOrElse("WAREHOUSE_TODO")
              val database =
                text(table.path("base_table"), "database").getOrElse("DATABASE_TODO")
              MSource(
                Seq(
                  s"Source = Snowflake.Databases(${mString(host)}, ${mString(warehouse)}),",
                  s"DB = Source{[Name=${mString(database)}]}[Data],"
                ),
                "DB"
              )
            case "postgresql" =>
              val (hostPort, database) = hostPortDb(info)
              MSource(
                Seq(s"Source = PostgreSQL.Database(${mString(hostPort)}, ${mString(database)}),"),
                "Source"
              )
            case "redshift" =>
              val (hostPort, database) = hostPortDb(info)
              MSource(
                Seq(
                  s"Source = AmazonRedshift.Database(${mString(hostPort)}, ${mString(database)}),"
                ),
                "Source"
              )
            case "mysql" =>
              val (hostPort, database) = hostPortDb(info)
              MSource(
                Seq(s"Source = MySQL.Database(${mString(hostPort)}, ${mString(database)}),"),
                "Source"
              )
            case "sqlserver" =>
              val (hostPort, database) = hostPortDb(info)
              val host = hostPort.split(':')(0)
              MSource(
                Seq(s"Source = Sql.Database(${mString(host)}, ${mString(database)}),"),
                "Source"
              )
            case other =>
              logger.warn(
                s"No Power Query connector mapping for engine '$other', emitting generic source"
              )
              fallback
          }
      }

    private def engineOf(info: ConnectionInfo): String =
      if (info.isBigQuery()) "bigquery"
      else {
        val url = info.options.getOrElse("url", "")
        if (url.startsWith("jdbc:")) {
          val engine = url.split(':')(1).toLowerCase
          if (engine == "mariadb") "mysql" else engine
        } else if (info.options.contains("sfUrl")) "snowflake"
        else "unknown"
      }

    private def hostOf(info: ConnectionInfo): Option[String] =
      info.options.getOrElse("url", "") match {
        case UrlPattern(host, _, _) => Some(host)
        case _                      => info.options.get("sfUrl").filter(_.nonEmpty)
      }

    private def hostPortDb(info: ConnectionInfo): (String, String) =
      info.options.getOrElse("url", "") match {
        case UrlPattern(host, port, db) =>
          val hostPort = Option(port).map(p => s"$host:$p").getOrElse(host)
          (hostPort, Option(db).getOrElse("DATABASE_TODO"))
        case _ => ("SERVER_TODO", "DATABASE_TODO")
      }

    private val fallback: MSource =
      MSource(
        Seq(
          "// TODO Starlake: set the connector for your warehouse",
          "Source = Sql.Database(\"SERVER_TODO\", \"DATABASE_TODO\"),"
        ),
        "Source"
      )
  }

  // ── helpers ──────────────────────────────────────────────────────────

  /** TMDL names are kept verbatim; quote when not a plain [A-Za-z0-9_]+ identifier. */
  private def quoteName(name: String): String =
    if (PlainName.matches(name)) name else s"'${name.replace("'", "''")}'"

  private def singleLine(s: String): String = s.replaceAll("\\s+", " ").trim

  /** Map semantic data_type to the TMDL dataType enum; unknown or absent types become string. */
  private def tmdlType(raw: Option[String]): String =
    raw.map(_.trim.toUpperCase).getOrElse("") match {
      case "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => "int64"
      case "NUMBER" | "NUMERIC" | "DECIMAL"          => "decimal"
      case "FLOAT" | "DOUBLE" | "REAL"               => "double"
      case "TEXT" | "STRING" | "VARCHAR" | "CHAR"    => "string"
      case "BOOLEAN" | "BOOL"                        => "boolean"
      case "DATE" | "DATETIME" | "TIME" | "TIMESTAMP" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" |
          "TIMESTAMP_TZ" =>
        "dateTime"
      case _ => "string"
    }
}
