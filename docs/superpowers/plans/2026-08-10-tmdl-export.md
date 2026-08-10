# Power BI TMDL Semantic Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `tmdl` format to the `semantic-export` command that converts Snowflake-style semantic models in `metadata/semantic/` into a Power BI TMDL folder (database.tmdl, model.tmdl, relationships.tmdl, tables/*.tmdl).

**Architecture:** Format-neutral helpers (`parseAggregate`, model-metric ownership, JsonNode accessors) are extracted from `LookMLConverter` into a new `SemanticModelOps` object; a new pure `TMDLConverter` renders the TMDL files from the parsed model `JsonNode` plus an `Option[ConnectionInfo]` that drives Power Query (M) source derivation. `SemanticExportJob` gains a `tmdl` dispatch case; ossie and lookml behavior is unchanged.

**Tech Stack:** Scala 2.13.18, Jackson `JsonNode` (via `YamlSerde.mapper`), scopt CLI, ScalaTest (`AnyFlatSpec` + `Matchers`, `TestHelper` for integration).

**Spec:** `docs/superpowers/specs/2026-08-09-tmdl-export-design.md`

## Global Constraints

- JDK 17, SBT 1.11.5, Scala 2.13.18. scalafmt runs automatically on compile; never hand-format against it.
- Tests run forked and sequentially; run single classes with `sbt "testOnly *ClassName*"` (slow, ~minutes). Compile errors count as the failing-test stage for new symbols.
- Work on branch `feat/semantic-export-lookml` (both semantic export formats ship together).
- The `docs/` path is gitignored; plan/spec files under `docs/superpowers/` need `git add -f`. Source and test files use plain `git add`.
- Every commit message ends with the trailer: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- TMDL indentation uses TAB characters (`\t`), not spaces. In Scala string literals write them as `\t` escapes so scalafmt cannot touch them.
- TMDL names are kept verbatim; a name is single-quoted in declarations/references when it does not match `[A-Za-z0-9_]+`. DAX table refs are always quoted (`'orders'`), `]` in DAX column refs escapes as `]]`, `"` in M strings escapes by doubling.
- Ossie (`OssieConverter`) is untouched by every task.

---

### Task 1: Extract SemanticModelOps (shared helpers refactor)

**Files:**
- Create: `src/main/scala/ai/starlake/semantic/SemanticModelOps.scala`
- Modify: `src/main/scala/ai/starlake/semantic/LookMLConverter.scala` (full replacement below)
- Modify: `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala:9` (import line only)

**Interfaces:**
- Consumes: current `LookMLConverter` internals (moved verbatim except where noted).
- Produces (all on `private[semantic] object SemanticModelOps`):
  - `case class ParsedAggregate(aggregate: String, arg: Option[String])` (field renamed from `lookmlType`; values unchanged: sum, average, min, max, count, count_distinct)
  - `def parseAggregate(expr: String): Option[ParsedAggregate]`
  - `val IdentifierPattern: Regex`, `val QualifiedPattern: Regex`
  - `def assignModelMetrics(model: JsonNode, tableNames: List[String], normalize: String => String): Map[String, List[(JsonNode, Boolean)]]` — keys are normalized table names; Boolean=false means the owning table could not be determined (metric mapped to the first table)
  - `def combinedDescription(node: JsonNode): Option[String]` — description with `Synonyms: a, b` appended
  - `def baseTableFqn(table: JsonNode): String` — `database.schema.table` from `base_table`, else the table name
  - `def elems(node: JsonNode, key: String): List[JsonNode]`, `def text(node: JsonNode, key: String): Option[String]`

This is a pure behavior-preserving refactor: no RED stage. The regression proof is the existing LookML/Ossie suites passing with only the moved-symbol import updated (assertions unchanged).

- [ ] **Step 1: Create SemanticModelOps**

Create `src/main/scala/ai/starlake/semantic/SemanticModelOps.scala`:

```scala
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

  /** description and synonyms combined into a single text: "<desc>. Synonyms: a, b", or either
    * part alone.
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
```

- [ ] **Step 2: Rewrite LookMLConverter to delegate**

Replace the entire content of `src/main/scala/ai/starlake/semantic/LookMLConverter.scala` with:

```scala
package ai.starlake.semantic

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer

/** Converts Snowflake-style semantic models to a LookML project layout: one view file per table
  * plus one model file holding the connection, includes and explores.
  */
object LookMLConverter extends LazyLogging {

  import SemanticModelOps._

  /** Convert one semantic model. Returns (relativePath, content) pairs: the model file first, then
    * one view file per table.
    */
  def convert(modelName: String, model: JsonNode, connection: String): Seq[(String, String)] = {
    val tables = elems(model, "tables")
    val metricsByView =
      assignModelMetrics(model, tables.map(_.path("name").asText()), sanitize)
    val views = tables.map { table =>
      val viewName = sanitize(table.path("name").asText())
      s"$viewName.view.lkml" -> renderView(table, metricsByView.getOrElse(viewName, Nil))
    }
    if (model.has("verified_queries"))
      logger.info(s"Model '$modelName': verified_queries have no LookML equivalent, skipped")
    (s"${sanitize(modelName)}.model.lkml" -> renderModel(model, connection)) +: views
  }

  private def renderView(table: JsonNode, modelMetrics: List[(JsonNode, Boolean)]): String = {
    val viewName = sanitize(table.path("name").asText())
    val pkColumns = elems(table.path("primary_key"), "columns").map(c => sanitize(c.asText()))
    val singlePk = if (pkColumns.size == 1) pkColumns.headOption else None
    // time_dimensions are excluded: a dimension_group only generates suffixed fields
    // (order_date_raw, order_date_date, ...), so ${order_date} would be invalid LookML.
    val fieldNames =
      (elems(table, "dimensions") ++ elems(table, "facts"))
        .map(f => sanitize(f.path("name").asText()))

    val lines = ArrayBuffer[String]()
    lines += s"view: $viewName {"
    lines += s"  sql_table_name: ${baseTableFqn(table)} ;;"
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
    elems(table, "metrics").foreach { m =>
      lines += ""
      lines ++= measure(m, viewName, fieldNames, owned = true)
    }
    modelMetrics.foreach { case (m, owned) =>
      lines += ""
      lines ++= measure(m, viewName, fieldNames, owned)
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
    combinedDescription(col).foreach(d => lines += s"    description: ${quote(d)}")
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
    combinedDescription(col).foreach(d => lines += s"    description: ${quote(d)}")
    lines += "  }"
    lines.toSeq
  }

  /** `owned` is false only for model-level metrics whose owning table could not be determined (see
    * SemanticModelOps.assignModelMetrics); such metrics always get the fallback treatment below,
    * even when `parseAggregate` succeeds, since rendering them as a native measure in an arbitrary
    * view would be silently wrong.
    */
  private def measure(
    m: JsonNode,
    viewName: String,
    fieldNames: List[String],
    owned: Boolean
  ): Seq[String] = {
    val name = sanitize(m.path("name").asText())
    val expr = m.path("expr").asText()
    val lines = ArrayBuffer[String]()
    (if (owned) parseAggregate(expr) else None) match {
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
    combinedDescription(m).foreach(d => lines += s"    description: ${quote(d)}")
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

  private def dimensionType(raw: String): Option[String] =
    raw.trim.toUpperCase match {
      case "NUMBER" | "NUMERIC" | "DECIMAL" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "FLOAT" |
          "DOUBLE" | "REAL" =>
        Some("number")
      case "TEXT" | "STRING" | "VARCHAR" | "CHAR" => Some("string")
      case "BOOLEAN" | "BOOL"                     => Some("yesno")
      case _                                      => None
    }

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
```

Removed from this file (now in `SemanticModelOps`): `ParsedAggregate`, the four regexes, `parseAggregate`, `assignModelMetrics`, `description` (renamed `combinedDescription`), `source` (renamed `baseTableFqn`), `elems`, `text`, and the imports they needed (`scala.jdk.CollectionConverters._`).

- [ ] **Step 3: Update the moved-symbol import in the spec**

In `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala`, replace line 9:

```scala
  import LookMLConverter.{parseAggregate, sanitize, ParsedAggregate}
```

with:

```scala
  import LookMLConverter.sanitize
  import SemanticModelOps.{parseAggregate, ParsedAggregate}
```

No assertion changes anywhere.

- [ ] **Step 4: Run the regression suites**

Run: `sbt "testOnly *SemanticExportSpec* *LookMLConverterSpec* *SemanticExportCmdSpec* *LookMLExportSpec*"`
Expected: PASS, 23 tests, 4 suites, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/SemanticModelOps.scala src/main/scala/ai/starlake/semantic/LookMLConverter.scala src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala
git commit -m "refactor(semantic): extract format-neutral helpers into SemanticModelOps

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: TMDLConverter skeleton (database, model, table columns)

**Files:**
- Create: `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`
- Test: `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala` (create)

**Interfaces:**
- Consumes: `SemanticModelOps` members from Task 1; `ai.starlake.config.ConnectionInfo` (case class with `` `type`: ConnectionType `` and `options: Map[String, String]`, all other fields defaulted).
- Produces:
  - `TMDLConverter.convert(modelName: String, model: JsonNode, connection: Option[ConnectionInfo]): Seq[(String, String)]` — relative paths `database.tmdl`, `model.tmdl`, `tables/<table>.tmdl` (relationships.tmdl arrives in Task 5; measures in Task 4; partitions in Task 3)
  - `private[semantic] case class GeneratedKey(table: String, columnName: String, sourceColumns: List[String])` — rendered in Task 5; the `renderTable` signature carries it from the start so later tasks only grow bodies
  - private helpers later tasks reuse: `quoteName(name: String): String`, `singleLine(s: String): String`, `tmdlType(raw: Option[String]): String`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`:

```scala
package ai.starlake.semantic

import ai.starlake.utils.YamlSerde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TMDLConverterSpec extends AnyFlatSpec with Matchers {

  private val modelYaml =
    """name: ecommerce_analytics
      |description: E-commerce analytics model
      |tables:
      |  - name: orders
      |    description: Order transactions
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: ORDERS
      |    dimensions:
      |      - name: order_id
      |        expr: ORDER_ID
      |        data_type: NUMBER
      |      - name: order_status
      |        data_type: TEXT
      |        synonyms: ["status"]
      |    time_dimensions:
      |      - name: order_date
      |        expr: ORDER_DATE
      |        data_type: DATE
      |    facts:
      |      - name: order_total
      |        expr: ORDER_TOTAL
      |        data_type: NUMBER
      |    metrics:
      |      - name: avg_order_value
      |        expr: AVG(order_total)
      |        synonyms: ["AOV"]
      |      - name: order_count
      |        expr: COUNT(*)
      |      - name: last_order_date
      |        expr: MAX(order_date)
      |    filters:
      |      - name: recent_orders
      |        expr: order_date >= CURRENT_DATE - 30
      |    primary_key:
      |      columns: [order_id]
      |  - name: customers
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: CUSTOMERS
      |    dimensions:
      |      - name: customer_id
      |        expr: CUSTOMER_ID
      |        data_type: NUMBER
      |    primary_key:
      |      columns: [customer_id]
      |metrics:
      |  - name: customer_count
      |    expr: COUNT(DISTINCT customers.customer_id)
      |  - name: total_revenue
      |    expr: SUM(a)/NULLIF(SUM(b),0)
      |verified_queries:
      |  - name: top
      |    question: Who?
      |    sql: SELECT 1
      |""".stripMargin

  private def files(): Map[String, String] =
    TMDLConverter
      .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), None)
      .toMap

  "convert" should "produce database, model and one table file per table" in {
    files().keySet shouldBe Set(
      "database.tmdl",
      "model.tmdl",
      "tables/orders.tmdl",
      "tables/customers.tmdl"
    )
  }

  it should "render database.tmdl and model.tmdl" in {
    val db = files()("database.tmdl")
    db should include("database ecommerce_analytics")
    db should include("\tcompatibilityLevel: 1600")
    val model = files()("model.tmdl")
    model should include("/// E-commerce analytics model")
    model should include("model Model")
    model should include("\tculture: en-US")
  }

  it should "render columns with dataType, sourceColumn, summarizeBy and isKey" in {
    val orders = files()("tables/orders.tmdl")
    orders should include("/// Order transactions")
    orders should include("table orders")
    orders should include("\tcolumn order_id")
    orders should include("\t\tdataType: decimal")
    orders should include("\t\tisKey")
    orders should include("\t\tsourceColumn: order_id")
    orders should include("\t/// Synonyms: status")
    orders should include("\tcolumn order_status")
    orders should include("\t\tdataType: string")
    orders should include("\tcolumn order_date")
    orders should include("\t\tdataType: dateTime")
    orders should include("\tcolumn order_total")
    orders should include("\t\tsummarizeBy: sum")
    val customers = files()("tables/customers.tmdl")
    customers should include("\t\tisKey")
  }

  it should "give summarizeBy none to dimensions and time dimensions" in {
    val orders = files()("tables/orders.tmdl")
    // 3 non-fact columns -> three "none" lines; the single fact is "sum"
    orders.split("\n").count(_ == "\t\tsummarizeBy: none") shouldBe 3
    orders.split("\n").count(_ == "\t\tsummarizeBy: sum") shouldBe 1
  }

  it should "quote names that are not plain identifiers" in {
    val yaml =
      """name: Mixed Model
        |tables:
        |  - name: Order Items
        |    dimensions:
        |      - name: item id
        |        data_type: NUMBER
        |""".stripMargin
    val out = TMDLConverter.convert("Mixed Model", YamlSerde.mapper.readTree(yaml), None).toMap
    out.keySet should contain("tables/Order Items.tmdl")
    val table = out("tables/Order Items.tmdl")
    table should include("table 'Order Items'")
    table should include("\tcolumn 'item id'")
    out("database.tmdl") should include("database 'Mixed Model'")
  }

  it should "skip isKey for composite primary keys" in {
    val yaml =
      """name: composite
        |tables:
        |  - name: line_items
        |    dimensions:
        |      - name: order_id
        |        data_type: NUMBER
        |      - name: line_no
        |        data_type: NUMBER
        |    primary_key:
        |      columns: [order_id, line_no]
        |""".stripMargin
    val table =
      TMDLConverter.convert("composite", YamlSerde.mapper.readTree(yaml), None).toMap
        .apply("tables/line_items.tmdl")
    table should not include "isKey"
  }

  it should "default unknown data types to string" in {
    val yaml =
      """name: unknowns
        |tables:
        |  - name: t
        |    dimensions:
        |      - name: mystery
        |        data_type: GEOGRAPHY
        |      - name: untyped
        |""".stripMargin
    val table =
      TMDLConverter.convert("unknowns", YamlSerde.mapper.readTree(yaml), None).toMap
        .apply("tables/t.tmdl")
    table.split("\n").count(_ == "\t\tdataType: string") shouldBe 2
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: compile FAILURE with `object TMDLConverter is not a member of package ai.starlake.semantic`.

- [ ] **Step 3: Write the implementation**

Create `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`:

```scala
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
```

Note: `keys`, `modelMetrics`, and `connection` are intentionally unused in this task; Tasks 3-5 fill the corresponding rendering into `renderTable` without changing its signature.

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: PASS, 7 tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/TMDLConverter.scala src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala
git commit -m "feat(semantic): TMDL database, model and table column rendering

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Partitions and Power Query source derivation

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`
- Test: `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`

**Interfaces:**
- Consumes: `renderTable`, `quoteName`, helpers from Task 2; `ConnectionInfo(`type` = ConnectionType.JDBC | ConnectionType.BQ, options = Map(...))`.
- Produces: every table file ends with an import-mode M partition; `private[semantic] case class MSource(lines: Seq[String], queryTarget: String)` and nested `object PbiSource { def resolve(connection: Option[ConnectionInfo], table: JsonNode): MSource }`.

- [ ] **Step 1: Write the failing tests**

Add to `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`. First add the imports at the top of the file:

```scala
import ai.starlake.config.ConnectionInfo
import ai.starlake.schema.model.ConnectionType
```

Then append the tests inside the class:

```scala
  private def pg: Option[ConnectionInfo] = Some(
    ConnectionInfo(
      `type` = ConnectionType.JDBC,
      options = Map("url" -> "jdbc:postgresql://myhost:5432/mydb")
    )
  )

  "partitions" should "emit an import-mode native query with expr AS name projections" in {
    val orders =
      TMDLConverter.convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), pg).toMap
        .apply("tables/orders.tmdl")
    orders should include("\tpartition orders = m")
    orders should include("\t\tmode: import")
    orders should include("\t\tsource =")
    orders should include("\t\t\tlet")
    orders should include("\t\t\t\tSource = PostgreSQL.Database(\"myhost:5432\", \"mydb\"),")
    orders should include(
      "\t\t\t\tResult = Value.NativeQuery(Source, \"SELECT ORDER_ID AS order_id, order_status, ORDER_DATE AS order_date, ORDER_TOTAL AS order_total FROM ANALYTICS_DB.ECOMMERCE.ORDERS\")"
    )
    orders should include("\t\t\tin")
    orders should include("\t\t\t\tResult")
  }

  it should "emit SELECT * when the table has no fields and fall back to the table name" in {
    val yaml =
      """name: bare
        |tables:
        |  - name: audit_log
        |""".stripMargin
    val table = TMDLConverter.convert("bare", YamlSerde.mapper.readTree(yaml), pg).toMap
      .apply("tables/audit_log.tmdl")
    table should include("Value.NativeQuery(Source, \"SELECT * FROM audit_log\")")
  }

  it should "derive the snowflake source with database navigation" in {
    val sf = Some(
      ConnectionInfo(
        `type` = ConnectionType.JDBC,
        options = Map(
          "url"       -> "jdbc:snowflake://acme.snowflakecomputing.com",
          "warehouse" -> "COMPUTE_WH"
        )
      )
    )
    val orders =
      TMDLConverter.convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), sf).toMap
        .apply("tables/orders.tmdl")
    orders should include(
      "\t\t\t\tSource = Snowflake.Databases(\"acme.snowflakecomputing.com\", \"COMPUTE_WH\"),"
    )
    orders should include("\t\t\t\tDB = Source{[Name=\"ANALYTICS_DB\"]}[Data],")
    orders should include("Result = Value.NativeQuery(DB, ")
  }

  it should "derive the bigquery source from the billing project option" in {
    val bq = Some(
      ConnectionInfo(`type` = ConnectionType.BQ, options = Map("gcpProjectId" -> "my-project"))
    )
    val orders =
      TMDLConverter.convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), bq).toMap
        .apply("tables/orders.tmdl")
    orders should include(
      "\t\t\t\tSource = GoogleBigQuery.Database([BillingProject=\"my-project\"]),"
    )
  }

  it should "fall back to a generic TODO source for missing or unmapped connections" in {
    val none =
      TMDLConverter.convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), None).toMap
        .apply("tables/orders.tmdl")
    none should include("\t\t\t\t// TODO Starlake: set the connector for your warehouse")
    none should include("\t\t\t\tSource = Sql.Database(\"SERVER_TODO\", \"DATABASE_TODO\"),")

    val duck = Some(
      ConnectionInfo(`type` = ConnectionType.JDBC, options = Map("url" -> "jdbc:duckdb:/tmp/db"))
    )
    val d =
      TMDLConverter.convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), duck).toMap
        .apply("tables/orders.tmdl")
    d should include("// TODO Starlake: set the connector for your warehouse")
  }

  it should "double embedded double quotes in the M query string" in {
    val yaml =
      """name: esc
        |tables:
        |  - name: t
        |    dimensions:
        |      - name: trimmed
        |        expr: TRIM("COL")
        |        data_type: TEXT
        |""".stripMargin
    val table = TMDLConverter.convert("esc", YamlSerde.mapper.readTree(yaml), pg).toMap
      .apply("tables/t.tmdl")
    table should include("SELECT TRIM(\"\"COL\"\") AS trimmed FROM t")
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: FAIL on the six new tests (no partition rendered yet); the Task 2 tests still PASS.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`:

1. In `renderTable`, after the last `elems(table, "facts").foreach(...)` line and before `lines.mkString("\n") + "\n"`, insert:

```scala
    lines ++= partition(table, connection)
```

2. After the `column` method, add:

```scala
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
        case None       => fallback
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
```

Note: `PbiSource` uses `text` and `mString` from the enclosing scope (`SemanticModelOps._` import and the `mString` defined above); it needs no own imports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: PASS, 13 tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/TMDLConverter.scala src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala
git commit -m "feat(semantic): TMDL partitions with connector-derived Power Query sources

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Measures (DAX translation)

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`
- Test: `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`

**Interfaces:**
- Consumes: `renderTable` and helpers from Tasks 2-3; `SemanticModelOps.{parseAggregate, ParsedAggregate, QualifiedPattern, IdentifierPattern, combinedDescription}`.
- Produces: measures rendered between the columns and the partition in each table file. Fallback text is exactly `\t/// TODO Starlake: translate original SQL to DAX: <expr>` followed by `\tmeasure <name> = BLANK()`.

- [ ] **Step 1: Write the failing tests**

Append to `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`:

```scala
  "measures" should "translate simple aggregates to DAX" in {
    val orders = files()("tables/orders.tmdl")
    orders should include("\t/// Synonyms: AOV")
    orders should include("\tmeasure avg_order_value = AVERAGE('orders'[order_total])")
    orders should include("\tmeasure order_count = COUNTROWS('orders')")
    orders should include("\tmeasure last_order_date = MAX('orders'[order_date])")
  }

  it should "attach owned model metrics to their table with DISTINCTCOUNT" in {
    val customers = files()("tables/customers.tmdl")
    customers should include("\tmeasure customer_count = DISTINCTCOUNT('customers'[customer_id])")
    files()("tables/orders.tmdl") should not include "customer_count"
  }

  it should "fall back to BLANK() with the original SQL for untranslatable metrics" in {
    val orders = files()("tables/orders.tmdl")
    orders should include(
      "\t/// TODO Starlake: translate original SQL to DAX: SUM(a)/NULLIF(SUM(b),0)"
    )
    orders should include("\tmeasure total_revenue = BLANK()")
  }

  it should "fall back when the aggregate argument is not a column of the table" in {
    val yaml =
      """name: stray
        |tables:
        |  - name: sales
        |    dimensions:
        |      - name: amount
        |        data_type: NUMBER
        |    metrics:
        |      - name: stray_sum
        |        expr: SUM(profit)
        |""".stripMargin
    val table = TMDLConverter.convert("stray", YamlSerde.mapper.readTree(yaml), None).toMap
      .apply("tables/sales.tmdl")
    table should include("\t/// TODO Starlake: translate original SQL to DAX: SUM(profit)")
    table should include("\tmeasure stray_sum = BLANK()")
  }

  it should "quote table and escape column names in DAX references" in {
    val yaml =
      """name: daxq
        |tables:
        |  - name: Order Items
        |    facts:
        |      - name: qty
        |        data_type: NUMBER
        |    metrics:
        |      - name: total_qty
        |        expr: SUM(qty)
        |""".stripMargin
    val table = TMDLConverter.convert("daxq", YamlSerde.mapper.readTree(yaml), None).toMap
      .apply("tables/Order Items.tmdl")
    table should include("\tmeasure total_qty = SUM('Order Items'[qty])")
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: FAIL on the five new tests (no measures rendered yet); earlier tests still PASS.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`:

1. In `renderTable`, replace the single inserted line from Task 3:

```scala
    lines ++= partition(table, connection)
```

with:

```scala
    val columnNames =
      (elems(table, "dimensions") ++ elems(table, "time_dimensions") ++ elems(table, "facts"))
        .map(_.path("name").asText())
    (elems(table, "metrics").map((_, true)) ++ modelMetrics).foreach { case (metric, owned) =>
      lines ++= measure(metric, name, columnNames, owned)
    }
    lines ++= partition(table, connection)
```

2. After the `column` method, add:

```scala
  /** DAX translation for simple aggregates; anything else (unparseable expression, argument not a
    * column of this table, or unowned model metric) falls back to BLANK() with the original SQL in
    * a TODO doc line. Unlike LookML, time dimension columns are real TMDL columns, so they are
    * valid DAX references.
    */
  private def measure(
    m: JsonNode,
    tableName: String,
    columnNames: List[String],
    owned: Boolean
  ): Seq[String] = {
    val name = m.path("name").asText()
    val expr = m.path("expr").asText()

    val dax: Option[String] = (if (owned) parseAggregate(expr) else None).flatMap { parsed =>
      parsed.aggregate match {
        case "count" => Some(s"COUNTROWS(${daxTable(tableName)})")
        case agg =>
          parsed.arg.flatMap { raw =>
            val local = raw.trim match {
              case QualifiedPattern(table, col) if table.equalsIgnoreCase(tableName) => col
              case other                                                             => other
            }
            columnNames
              .find(c => IdentifierPattern.matches(local) && c.equalsIgnoreCase(local))
              .flatMap { columnName =>
                val ref = daxColumn(tableName, columnName)
                agg match {
                  case "sum"            => Some(s"SUM($ref)")
                  case "average"        => Some(s"AVERAGE($ref)")
                  case "min"            => Some(s"MIN($ref)")
                  case "max"            => Some(s"MAX($ref)")
                  case "count_distinct" => Some(s"DISTINCTCOUNT($ref)")
                  case _                => None
                }
              }
          }
      }
    }

    val lines = ArrayBuffer[String]()
    lines += ""
    combinedDescription(m).foreach(d => lines += s"\t/// ${singleLine(d)}")
    dax match {
      case Some(d) =>
        lines += s"\tmeasure ${quoteName(name)} = $d"
      case None =>
        lines += s"\t/// TODO Starlake: translate original SQL to DAX: ${singleLine(expr)}"
        lines += s"\tmeasure ${quoteName(name)} = BLANK()"
    }
    lines.toSeq
  }

  /** DAX table reference: always single-quoted. */
  private def daxTable(name: String): String = s"'${name.replace("'", "''")}'"

  /** DAX column reference: 'table'[column] with ] escaped as ]]. */
  private def daxColumn(table: String, column: String): String =
    s"${daxTable(table)}[${column.replace("]", "]]")}]"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: PASS, 18 tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/TMDLConverter.scala src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala
git commit -m "feat(semantic): TMDL measures with DAX translation and BLANK fallback

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Relationships and composite concatenated keys

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`
- Test: `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`

**Interfaces:**
- Consumes: `convert`, `renderTable(table, keys, modelMetrics, connection)`, `GeneratedKey`, `quoteName` from earlier tasks.
- Produces: `relationships.tmdl` emitted when at least one valid relationship exists; composite relationships generate hidden `_sl_<relname>_key` COMBINEVALUES columns on both tables.

- [ ] **Step 1: Write the failing tests**

Append to `src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala`:

```scala
  private val relationshipYaml =
    """name: rels
      |tables:
      |  - name: orders
      |    dimensions:
      |      - name: customer_id
      |        data_type: NUMBER
      |      - name: order_id
      |        data_type: NUMBER
      |      - name: company_id
      |        data_type: NUMBER
      |  - name: customers
      |    dimensions:
      |      - name: customer_id
      |        data_type: NUMBER
      |  - name: line_items
      |    dimensions:
      |      - name: order_id
      |        data_type: NUMBER
      |      - name: company_id
      |        data_type: NUMBER
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |    join_type: left_outer
      |    relationship_type: many_to_one
      |  - name: items_to_orders
      |    left_table: line_items
      |    right_table: orders
      |    relationship_columns:
      |      - left_column: order_id
      |        right_column: order_id
      |      - left_column: company_id
      |        right_column: company_id
      |  - name: broken
      |    left_table: orders
      |    right_table: customers
      |""".stripMargin

  private def relFiles(): Map[String, String] =
    TMDLConverter.convert("rels", YamlSerde.mapper.readTree(relationshipYaml), None).toMap

  "relationships" should "render single-column relationships" in {
    val rels = relFiles()("relationships.tmdl")
    rels should include("relationship orders_to_customers")
    rels should include("\tfromColumn: orders.customer_id")
    rels should include("\ttoColumn: customers.customer_id")
  }

  it should "generate hidden COMBINEVALUES key columns for composite relationships" in {
    val items = relFiles()("tables/line_items.tmdl")
    items should include(
      "\tcolumn _sl_items_to_orders_key = COMBINEVALUES(\"|\", [order_id], [company_id])"
    )
    items should include("\t\tisHidden")
    val orders = relFiles()("tables/orders.tmdl")
    orders should include(
      "\tcolumn _sl_items_to_orders_key = COMBINEVALUES(\"|\", [order_id], [company_id])"
    )
    val rels = relFiles()("relationships.tmdl")
    rels should include("relationship items_to_orders")
    rels should include("\tfromColumn: line_items._sl_items_to_orders_key")
    rels should include("\ttoColumn: orders._sl_items_to_orders_key")
  }

  it should "skip relationships without columns and omit the file when none are valid" in {
    relFiles()("relationships.tmdl") should not include "broken"

    val yaml =
      """name: norel
        |tables:
        |  - name: t
        |relationships:
        |  - name: broken
        |    left_table: t
        |    right_table: t
        |""".stripMargin
    val out = TMDLConverter.convert("norel", YamlSerde.mapper.readTree(yaml), None).toMap
    out.keySet should not contain "relationships.tmdl"
  }

  it should "not emit relationships.tmdl when the model has no relationships" in {
    files().keySet should not contain "relationships.tmdl"
  }

  it should "quote non-plain table names in relationship references" in {
    val yaml =
      """name: q
        |tables:
        |  - name: Order Items
        |    dimensions:
        |      - name: order_id
        |        data_type: NUMBER
        |  - name: orders
        |    dimensions:
        |      - name: order_id
        |        data_type: NUMBER
        |relationships:
        |  - name: items_to_orders
        |    left_table: Order Items
        |    right_table: orders
        |    relationship_columns:
        |      - left_column: order_id
        |        right_column: order_id
        |""".stripMargin
    val rels = TMDLConverter.convert("q", YamlSerde.mapper.readTree(yaml), None).toMap
      .apply("relationships.tmdl")
    rels should include("\tfromColumn: 'Order Items'.order_id")
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: FAIL on the five new tests (no relationships.tmdl produced yet, no key columns); earlier tests still PASS.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/ai/starlake/semantic/TMDLConverter.scala`:

1. Replace the body of `convert` with:

```scala
    val tables = elems(model, "tables")
    val metricsByTable =
      assignModelMetrics(model, tables.map(_.path("name").asText()), _.toLowerCase)
    val (relEntries, generatedKeys) = relationshipPlan(elems(model, "relationships"))
    if (model.has("verified_queries"))
      logger.info(s"Model '$modelName': verified_queries have no TMDL equivalent, skipped")

    val files = ArrayBuffer[(String, String)]()
    files += "database.tmdl" -> renderDatabase(modelName)
    files += "model.tmdl" -> renderModelFile(model)
    if (relEntries.nonEmpty)
      files += "relationships.tmdl" -> renderRelationships(relEntries)
    tables.foreach { table =>
      val name = table.path("name").asText()
      files += s"tables/$name.tmdl" -> renderTable(
        table,
        keys = generatedKeys.filter(_.table.equalsIgnoreCase(name)),
        modelMetrics = metricsByTable.getOrElse(name.toLowerCase, Nil),
        connection = connection
      )
    }
    files.toSeq
```

2. After `GeneratedKey`, add:

```scala
  private case class RelEntry(
    name: String,
    fromTable: String,
    fromColumn: String,
    toTable: String,
    toColumn: String
  )

  /** Split relationships into direct single-column entries and composite ones, which get hidden
    * COMBINEVALUES key columns generated on both tables. Relationships without column pairs are
    * skipped with a warning. relationship_type values other than many_to_one (the engine default)
    * are logged and emitted with default cardinality; join_type has no TMDL equivalent.
    */
  private def relationshipPlan(
    relationships: List[JsonNode]
  ): (List[RelEntry], List[GeneratedKey]) = {
    val entries = ArrayBuffer[RelEntry]()
    val keys = ArrayBuffer[GeneratedKey]()
    relationships.foreach { rel =>
      val name = rel.path("name").asText()
      val left = rel.path("left_table").asText()
      val right = rel.path("right_table").asText()
      text(rel, "relationship_type").filterNot(_.equalsIgnoreCase("many_to_one")).foreach { t =>
        logger.warn(
          s"Relationship '$name': relationship_type '$t' has no direct TMDL mapping, emitting default cardinality"
        )
      }
      val pairs = elems(rel, "relationship_columns").flatMap { rc =>
        for {
          l <- text(rc, "left_column")
          r <- text(rc, "right_column")
        } yield (l, r)
      }
      pairs match {
        case Nil =>
          logger.warn(s"Relationship '$name' has no relationship_columns, skipped")
        case (l, r) :: Nil =>
          entries += RelEntry(name, left, l, right, r)
        case many =>
          val keyName = s"_sl_${name}_key"
          keys += GeneratedKey(left, keyName, many.map(_._1))
          keys += GeneratedKey(right, keyName, many.map(_._2))
          entries += RelEntry(name, left, keyName, right, keyName)
      }
    }
    (entries.toList, keys.toList)
  }

  private def renderRelationships(entries: List[RelEntry]): String = {
    val lines = ArrayBuffer[String]()
    entries.foreach { e =>
      if (lines.nonEmpty) lines += ""
      lines += s"relationship ${quoteName(e.name)}"
      lines += s"\tfromColumn: ${quoteName(e.fromTable)}.${quoteName(e.fromColumn)}"
      lines += s"\ttoColumn: ${quoteName(e.toTable)}.${quoteName(e.toColumn)}"
    }
    lines.mkString("\n") + "\n"
  }
```

3. In `renderTable`, immediately after the third `elems(table, "facts").foreach(...)` line (before the `columnNames` block from Task 4), insert:

```scala
    keys.foreach { k =>
      lines += ""
      val refs = k.sourceColumns.map(c => s"[${c.replace("]", "]]")}]")
      lines += s"\tcolumn ${quoteName(k.columnName)} = COMBINEVALUES(${("\"|\"" +: refs).mkString(", ")})"
      lines += "\t\tdataType: string"
      lines += "\t\tisHidden"
      lines += "\t\tsummarizeBy: none"
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `sbt "testOnly *TMDLConverterSpec*"`
Expected: PASS, 23 tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/TMDLConverter.scala src/test/scala/ai/starlake/semantic/TMDLConverterSpec.scala
git commit -m "feat(semantic): TMDL relationships with composite concatenated keys

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: CLI surface, job dispatch and end-to-end export

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/SemanticExportCmd.scala`
- Modify: `src/main/scala/ai/starlake/semantic/SemanticExportJob.scala`
- Modify: `src/test/scala/ai/starlake/semantic/SemanticExportCmdSpec.scala`
- Test: `src/test/scala/ai/starlake/semantic/TMDLExportSpec.scala` (create)

**Interfaces:**
- Consumes: `TMDLConverter.convert(name, node, Option[ConnectionInfo])`; `config.connection: Option[String]`; `settings.appConfig.connectionRef: String`; `settings.appConfig.connections: Map[String, ConnectionInfo]` (verify the exact accessor in `Settings.scala` when implementing; it is the map the `connectionRef` indexes into).
- Produces: `starlake semantic-export --format tmdl` writes `export/tmdl/<modelName>/{database.tmdl,model.tmdl,relationships.tmdl,tables/*.tmdl}`. Ossie and lookml outputs unchanged.

- [ ] **Step 1: Write the failing tests**

Add to `src/test/scala/ai/starlake/semantic/SemanticExportCmdSpec.scala` inside the class:

```scala
  it should "accept the tmdl format" in {
    SemanticExportCmd.parse(Seq("--format", "tmdl", "--connection", "pg_conn")) shouldBe Some(
      SemanticExportConfig(format = "tmdl", connection = Some("pg_conn"))
    )
  }
```

Create `src/test/scala/ai/starlake/semantic/TMDLExportSpec.scala`:

```scala
package ai.starlake.semantic

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

class TMDLExportSpec extends TestHelper {

  private val modelYaml =
    """name: ecommerce_analytics
      |description: E-commerce analytics model
      |tables:
      |  - name: orders
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: ORDERS
      |    dimensions:
      |      - name: order_id
      |        expr: ORDER_ID
      |        data_type: NUMBER
      |      - name: customer_id
      |        expr: CUSTOMER_ID
      |        data_type: NUMBER
      |    facts:
      |      - name: order_total
      |        expr: ORDER_TOTAL
      |        data_type: NUMBER
      |    metrics:
      |      - name: avg_order_value
      |        expr: AVG(order_total)
      |    primary_key:
      |      columns: [order_id]
      |  - name: customers
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: CUSTOMERS
      |    dimensions:
      |      - name: customer_id
      |        expr: CUSTOMER_ID
      |        data_type: NUMBER
      |    primary_key:
      |      columns: [customer_id]
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |""".stripMargin

  "semantic-export --format tmdl" should "write a TMDL folder per model" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(
        SemanticExportConfig(format = "tmdl", connection = Some("unknown_connection"))
      ).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }

      val outDir = new Path(DatasetArea.semantic, "export/tmdl/ecommerce_analytics")
      storage.read(new Path(outDir, "database.tmdl")) should include(
        "database ecommerce_analytics"
      )
      storage.read(new Path(outDir, "model.tmdl")) should include("model Model")

      val orders = storage.read(new Path(outDir, "tables/orders.tmdl"))
      orders should include("table orders")
      orders should include("\t\tisKey")
      orders should include("\tmeasure avg_order_value = AVERAGE('orders'[order_total])")
      // unknown connection name falls back to the generic source
      orders should include("// TODO Starlake: set the connector for your warehouse")
      orders should include("FROM ANALYTICS_DB.ECOMMERCE.ORDERS")

      storage.read(new Path(outDir, "relationships.tmdl")) should include(
        "relationship orders_to_customers"
      )
      storage.read(new Path(outDir, "tables/customers.tmdl")) should include("table customers")
    }
  }

  it should "leave the ossie and lookml exports untouched" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(SemanticExportConfig()).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }
      storage.exists(
        new Path(DatasetArea.semantic, "export/ossie/ecommerce_analytics.ossie.yaml")
      ) shouldBe true

      new SemanticExportJob(
        SemanticExportConfig(format = "lookml", connection = Some("wh"))
      ).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }
      storage.exists(
        new Path(DatasetArea.semantic, "export/lookml/ecommerce_analytics/orders.view.lkml")
      ) shouldBe true
    }
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `sbt "testOnly *SemanticExportCmdSpec* *TMDLExportSpec*"`
Expected: `SemanticExportCmdSpec` FAILS on the new tmdl test (scopt rejects the format). `TMDLExportSpec` FAILS on the first test (job writes `<name>.ossie.yaml` for the tmdl format); the ossie/lookml test PASSES.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/ai/starlake/semantic/SemanticExportCmd.scala`:

1. Replace `pageDescription` with:

```scala
  override def pageDescription: String =
    "Export semantic models from metadata/semantic to Apache Ossie, a LookML project or a Power BI TMDL folder."
```

2. In `pageKeywords`, after `"looker",` add:

```scala
      "tmdl",
      "power bi",
```

3. Replace the `builder.note(...)` block with:

```scala
      builder.note(
        """
          |Export the semantic models stored in metadata/semantic/ to another semantic
          |format. Supported formats: ossie (Apache Ossie, incubating), lookml (a Looker
          |project: one view file per table plus a model file with explores) and tmdl
          |(a Power BI TMDL folder: database.tmdl, model.tmdl, relationships.tmdl and
          |one tables/<table>.tmdl per table).
          |
          |For ossie, Starlake-specific attributes with no Ossie counterpart are
          |preserved in custom_extensions blocks under the STARLAKE vendor name.
          |
          |For lookml, --connection sets the Looker connection name in the model file.
          |
          |For tmdl, --connection names the Starlake connection used to derive each
          |table's Power Query source; simple aggregate metrics are translated to DAX
          |and anything else becomes a BLANK() measure carrying the original SQL in a
          |TODO comment.
          |
          |example: starlake semantic-export
          |         --format tmdl
          |         --model ecommerce_analytics
          |         --connection snowflake_prod
          |         --output /tmp/tmdl-models""".stripMargin
      ),
```

4. Replace the `--format` option definition with:

```scala
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .validate(x =>
          if (Set("ossie", "lookml", "tmdl").contains(x)) builder.success
          else
            builder.failure(s"Unsupported format '$x'. Supported formats: ossie, lookml, tmdl")
        )
        .text("Target format: ossie (default), lookml or tmdl")
        .optional(),
```

5. Replace the `--connection` option's `.text(...)` with:

```scala
        .text(
          "lookml: Looker connection name written to the model file; tmdl: Starlake connection used to derive the Power Query source. Defaults to the project's connectionRef"
        )
```

In `src/main/scala/ai/starlake/semantic/SemanticExportJob.scala`:

6. Update the class scaladoc first sentence from "...interchange format or to a LookML project." to:

```scala
/** Exports semantic models stored in metadata/semantic/ to the Apache Ossie (incubating)
  * interchange format, a LookML project or a Power BI TMDL folder.
```

7. Replace the whole `selected.foreach { ... }` write loop with:

```scala
    selected.foreach { case (path, _, name, node) =>
      def writeAll(files: Seq[(String, String)]): Unit = {
        val modelDir = new Path(outputDir, name)
        storage.mkdirs(modelDir)
        files.foreach { case (relativePath, content) =>
          val target = new Path(modelDir, relativePath)
          storage.write(content, target)
          logger.info(s"Exported semantic model '$name' ($path) to $target")
        }
      }
      config.format match {
        case "lookml" =>
          val connection = config.connection.getOrElse(settings.appConfig.connectionRef)
          writeAll(LookMLConverter.convert(name, node, connection))
        case "tmdl" =>
          val connectionName = config.connection.getOrElse(settings.appConfig.connectionRef)
          val connectionInfo = settings.appConfig.connections.get(connectionName)
          if (connectionInfo.isEmpty)
            logger.warn(
              s"Connection '$connectionName' not found, TMDL partitions will use a generic source"
            )
          writeAll(TMDLConverter.convert(name, node, connectionInfo))
        case _ =>
          val ossie = OssieConverter.convert(name, node)
          val target = new Path(outputDir, s"$name.ossie.yaml")
          storage.write(YamlSerde.mapper.writeValueAsString(ossie), target)
          logger.info(s"Exported semantic model '$name' ($path) to $target")
      }
    }
```

(The lookml branch is a pure refactor of its previous body into `writeAll`; output is byte-identical.)

- [ ] **Step 4: Run the new suites**

Run: `sbt "testOnly *SemanticExportCmdSpec* *TMDLExportSpec*"`
Expected: PASS.

- [ ] **Step 5: Run the full semantic regression**

Run: `sbt "testOnly *SemanticExportSpec* *LookMLConverterSpec* *SemanticExportCmdSpec* *LookMLExportSpec* *TMDLConverterSpec* *TMDLExportSpec*"`
Expected: PASS, 6 suites, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/SemanticExportCmd.scala src/main/scala/ai/starlake/semantic/SemanticExportJob.scala src/test/scala/ai/starlake/semantic/SemanticExportCmdSpec.scala src/test/scala/ai/starlake/semantic/TMDLExportSpec.scala
git commit -m "feat(semantic): export semantic models as Power BI TMDL folders

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
