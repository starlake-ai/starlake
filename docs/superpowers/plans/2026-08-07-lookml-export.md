# LookML Semantic Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `lookml` format to the `semantic-export` command that converts Snowflake-style semantic models in `metadata/semantic/` into a Looker project (one view file per table plus a model file with explores).

**Architecture:** A new pure `LookMLConverter` object (sibling of `OssieConverter`) turns the parsed model `JsonNode` into `Seq[(relativePath, content)]` file pairs. `SemanticExportJob` dispatches on `config.format` and writes the LookML files under `export/lookml/<modelName>/`. A new `--connection` CLI flag fills the model file's `connection:` line, defaulting to `settings.appConfig.connectionRef`.

**Tech Stack:** Scala 2.13.18, Jackson `JsonNode` (via `YamlSerde.mapper`), scopt CLI parsing, ScalaTest (`AnyFlatSpec` + `Matchers`, `TestHelper` for integration).

**Spec:** `docs/superpowers/specs/2026-08-07-lookml-export-design.md`

## Global Constraints

- JDK 17, SBT 1.11.5, Scala 2.13.18. Single SBT module.
- scalafmt runs automatically on compile; never hand-format against it. Run `sbt scalafmtCheck` if in doubt.
- Tests run sequentially and forked; run single classes with `sbt "testOnly *ClassName*"`. Expect slow (~minutes) cycles; compile errors count as the "failing test" stage for new symbols.
- The `docs/` path is gitignored in this repo; plan/spec files under `docs/superpowers/` are force-added (`git add -f`). Source and test files are NOT ignored, use plain `git add`.
- Work on branch `feat/semantic-export-lookml`.
- Every commit message ends with the trailer: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- LookML identifiers must match `[a-z0-9_]+`; all emitted names are sanitized (lowercase, invalid chars to `_`).

---

### Task 1: LookMLConverter helpers (sanitize + aggregate parsing)

**Files:**
- Create: `src/main/scala/ai/starlake/semantic/LookMLConverter.scala`
- Test: `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala`

**Interfaces:**
- Consumes: nothing (pure functions).
- Produces:
  - `LookMLConverter.sanitize(name: String): String` (`private[semantic]`)
  - `LookMLConverter.ParsedAggregate(lookmlType: String, arg: Option[String])` (`private[semantic]` case class)
  - `LookMLConverter.parseAggregate(expr: String): Option[ParsedAggregate]` (`private[semantic]`)

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala`:

```scala
package ai.starlake.semantic

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LookMLConverterSpec extends AnyFlatSpec with Matchers {

  import LookMLConverter.{parseAggregate, sanitize, ParsedAggregate}

  "sanitize" should "lowercase and replace invalid characters" in {
    sanitize("order_id") shouldBe "order_id"
    sanitize("Order Status") shouldBe "order_status"
    sanitize("CUSTOMERS") shouldBe "customers"
    sanitize("weird-name.1") shouldBe "weird_name_1"
  }

  "parseAggregate" should "map simple aggregates to native LookML measure types" in {
    parseAggregate("SUM(order_total)") shouldBe Some(ParsedAggregate("sum", Some("order_total")))
    parseAggregate("avg(order_total)") shouldBe Some(
      ParsedAggregate("average", Some("order_total"))
    )
    parseAggregate("MIN(x)") shouldBe Some(ParsedAggregate("min", Some("x")))
    parseAggregate("MAX(x)") shouldBe Some(ParsedAggregate("max", Some("x")))
    parseAggregate("COUNT(*)") shouldBe Some(ParsedAggregate("count", None))
    parseAggregate("COUNT(DISTINCT customers.customer_id)") shouldBe Some(
      ParsedAggregate("count_distinct", Some("customers.customer_id"))
    )
  }

  it should "reject expressions that are not a single simple aggregate" in {
    parseAggregate("SUM(a)/NULLIF(SUM(b),0)") shouldBe None
    parseAggregate("SUM(a) + SUM(b)") shouldBe None
    parseAggregate("COUNT(order_id)") shouldBe None
    parseAggregate("CASE WHEN x THEN 1 ELSE 0 END") shouldBe None
    parseAggregate("order_total") shouldBe None
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly *LookMLConverterSpec*"`
Expected: compile FAILURE with `object LookMLConverter is not a member of package ai.starlake.semantic` (compile error is the failing stage for a new object).

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/ai/starlake/semantic/LookMLConverter.scala`:

```scala
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
```

Note: `COUNT(order_id)` (count of non-null values) has no direct LookML measure type, so it intentionally falls to `None` and gets the `type: number` fallback treatment in Task 2.

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly *LookMLConverterSpec*"`
Expected: PASS, 3 tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/LookMLConverter.scala src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala
git commit -m "feat(semantic): LookML identifier sanitization and aggregate parsing

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: View file rendering and `convert` entry point

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/LookMLConverter.scala`
- Test: `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala`

**Interfaces:**
- Consumes: `sanitize`, `parseAggregate`, `ParsedAggregate` from Task 1.
- Produces:
  - `LookMLConverter.convert(modelName: String, model: JsonNode, connection: String): Seq[(String, String)]` — public entry point returning (relativePath, content) pairs. In this task the model file content is a stub (connection + include only); Task 3 adds explores.

- [ ] **Step 1: Write the failing test**

Add to `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala` (inside the class body, after the existing tests). Also add the import at the top of the file:

```scala
import ai.starlake.utils.YamlSerde
```

New tests:

```scala
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
      |        unique: true
      |      - name: order_status
      |        expr: ORDER_STATUS
      |        data_type: TEXT
      |        is_enum: true
      |        synonyms: ["status"]
      |        sample_values: ["Pending", "Shipped"]
      |    time_dimensions:
      |      - name: order_date
      |        expr: ORDER_DATE
      |        data_type: DATE
      |    facts:
      |      - name: order_total
      |        expr: ORDER_TOTAL
      |        data_type: NUMBER
      |        access_modifier: public_access
      |    metrics:
      |      - name: avg_order_value
      |        expr: AVG(order_total)
      |        synonyms: ["AOV"]
      |    filters:
      |      - name: recent_orders
      |        expr: order_date >= DATEADD(day, -30, CURRENT_DATE())
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
      |        unique: true
      |    primary_key:
      |      columns: [customer_id]
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |    join_type: left_outer
      |    relationship_type: many_to_one
      |metrics:
      |  - name: customer_count
      |    expr: COUNT(DISTINCT customers.customer_id)
      |verified_queries:
      |  - name: top_customers
      |    question: Who are the top 10 customers by revenue?
      |    sql: SELECT 1
      |""".stripMargin

  private def convertSample(): Map[String, String] =
    LookMLConverter
      .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), "analytics_wh")
      .toMap

  "convert" should "produce one view file per table plus a model file" in {
    val files = convertSample()
    files.keySet shouldBe Set(
      "ecommerce_analytics.model.lkml",
      "orders.view.lkml",
      "customers.view.lkml"
    )
  }

  it should "render dimensions, primary key, suggestions and sql_table_name" in {
    val orders = convertSample()("orders.view.lkml")
    orders should include("view: orders {")
    orders should include("sql_table_name: ANALYTICS_DB.ECOMMERCE.ORDERS ;;")
    orders should include("dimension: order_id {")
    orders should include("primary_key: yes")
    orders should include("type: number")
    orders should include("sql: ORDER_ID ;;")
    orders should include("dimension: order_status {")
    orders should include("type: string")
    orders should include("""description: "Synonyms: status"""")
    orders should include("""suggestions: ["Pending", "Shipped"]""")
    orders should include("# filters (not translated to LookML):")
    orders should include("#   recent_orders: order_date >= DATEADD(day, -30, CURRENT_DATE())")
  }

  it should "render time dimensions as dimension_groups" in {
    val orders = convertSample()("orders.view.lkml")
    orders should include("dimension_group: order_date {")
    orders should include("type: time")
    orders should include("timeframes: [raw, date, week, month, quarter, year]")
    orders should include("datatype: date")
    orders should include("sql: ORDER_DATE ;;")
  }

  it should "render table metrics as native measures referencing view fields" in {
    val orders = convertSample()("orders.view.lkml")
    orders should include("measure: avg_order_value {")
    orders should include("type: average")
    orders should include("sql: ${order_total} ;;")
    orders should include("""description: "Synonyms: AOV"""")
  }

  it should "attach model-level metrics to the view owning the aggregate argument" in {
    val customers = convertSample()("customers.view.lkml")
    customers should include("measure: customer_count {")
    customers should include("type: count_distinct")
    customers should include("sql: ${customer_id} ;;")
    val orders = convertSample()("orders.view.lkml")
    orders should not include "measure: customer_count"
  }

  it should "fall back to type number with raw SQL for unmappable metrics" in {
    val yaml =
      """name: fallback_model
        |tables:
        |  - name: sales
        |    dimensions:
        |      - name: a
        |        data_type: NUMBER
        |    metrics:
        |      - name: weird_ratio
        |        expr: SUM(a)/NULLIF(SUM(b),0)
        |""".stripMargin
    val files =
      LookMLConverter.convert("fallback_model", YamlSerde.mapper.readTree(yaml), "wh").toMap
    val sales = files("sales.view.lkml")
    sales should include(
      "# starlake: verify this measure, expression could not be mapped to a native LookML type"
    )
    sales should include("measure: weird_ratio {")
    sales should include("type: number")
    sales should include("sql: SUM(a)/NULLIF(SUM(b),0) ;;")
    // no base_table: sql_table_name falls back to the table name
    sales should include("sql_table_name: sales ;;")
  }

  it should "sanitize mixed-case table and column names" in {
    val yaml =
      """name: Mixed Model
        |tables:
        |  - name: Order Items
        |    dimensions:
        |      - name: Item-Id
        |        data_type: NUMBER
        |""".stripMargin
    val files = LookMLConverter.convert("Mixed Model", YamlSerde.mapper.readTree(yaml), "wh").toMap
    files.keySet should contain("order_items.view.lkml")
    files.keySet should contain("mixed_model.model.lkml")
    files("order_items.view.lkml") should include("dimension: item_id {")
  }

  it should "skip primary_key flag for composite primary keys with a comment" in {
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
    val files = LookMLConverter.convert("composite", YamlSerde.mapper.readTree(yaml), "wh").toMap
    val view = files("line_items.view.lkml")
    view should include("# composite primary key (order_id, line_no) not representable in LookML")
    view should not include "primary_key: yes"
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly *LookMLConverterSpec*"`
Expected: compile FAILURE with `value convert is not a member of object ai.starlake.semantic.LookMLConverter`.

- [ ] **Step 3: Write the implementation**

Replace the entire content of `src/main/scala/ai/starlake/semantic/LookMLConverter.scala` with:

```scala
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

  /** Model-level metrics are attached to the view owning the aggregate argument
    * (COUNT(DISTINCT customers.customer_id) lands in the customers view); metrics whose owning
    * table cannot be determined go to the first view.
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
          arg <- parsed.arg
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

  /** Placeholder completed in Task 3 (explores). */
  private def renderModel(model: JsonNode, connection: String): String =
    s"""connection: ${quote(connection)}
       |include: "*.view.lkml"
       |""".stripMargin

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly *LookMLConverterSpec*"`
Expected: PASS, all tests succeeded (Task 1 tests still green).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/LookMLConverter.scala src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala
git commit -m "feat(semantic): render LookML view files from semantic model tables

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Model file with explores

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/LookMLConverter.scala` (replace the `renderModel` placeholder)
- Test: `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala`

**Interfaces:**
- Consumes: `convert`, `sanitize`, `quote`, `elems`, `text` from Task 2.
- Produces: final `renderModel` behavior; `convert`'s public signature is unchanged.

- [ ] **Step 1: Write the failing test**

Add to `src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala`:

```scala
  "renderModel" should "emit connection, include and one explore per left table" in {
    val model = convertSample()("ecommerce_analytics.model.lkml")
    model should include("""connection: "analytics_wh"""")
    model should include("""include: "*.view.lkml"""")
    model should include("explore: orders {")
    model should include("join: customers {")
    model should include("type: left_outer")
    model should include("relationship: many_to_one")
    model should include("sql_on: ${orders.customer_id} = ${customers.customer_id} ;;")
    // customers is joined from orders, no explore of its own
    model should not include "explore: customers"
  }

  it should "emit bare explores for tables in no relationship and default join attributes" in {
    val yaml =
      """name: multi
        |tables:
        |  - name: orders
        |  - name: customers
        |  - name: audit_log
        |relationships:
        |  - name: orders_to_customers
        |    left_table: orders
        |    right_table: customers
        |    relationship_columns:
        |      - left_column: customer_id
        |        right_column: customer_id
        |""".stripMargin
    val model =
      LookMLConverter.convert("multi", YamlSerde.mapper.readTree(yaml), "wh").toMap("multi.model.lkml")
    model should include("explore: audit_log {}")
    // join_type / relationship_type absent: defaults apply
    model should include("type: left_outer")
    model should include("relationship: many_to_one")
  }

  it should "join composite relationship columns with AND" in {
    val yaml =
      """name: composite_rel
        |tables:
        |  - name: line_items
        |  - name: orders
        |relationships:
        |  - name: items_to_orders
        |    left_table: line_items
        |    right_table: orders
        |    relationship_columns:
        |      - left_column: order_id
        |        right_column: order_id
        |      - left_column: company_id
        |        right_column: company_id
        |""".stripMargin
    val model = LookMLConverter
      .convert("composite_rel", YamlSerde.mapper.readTree(yaml), "wh")
      .toMap("composite_rel.model.lkml")
    model should include(
      "sql_on: ${line_items.order_id} = ${orders.order_id} AND ${line_items.company_id} = ${orders.company_id} ;;"
    )
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly *LookMLConverterSpec*"`
Expected: FAIL on the three new tests (the placeholder model file has no explores), all earlier tests PASS.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/ai/starlake/semantic/LookMLConverter.scala`, replace the `renderModel` placeholder method with:

```scala
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly *LookMLConverterSpec*"`
Expected: PASS, all tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/LookMLConverter.scala src/test/scala/ai/starlake/semantic/LookMLConverterSpec.scala
git commit -m "feat(semantic): render LookML model file with explores and joins

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: CLI surface (`--format lookml`, `--connection`)

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/SemanticExportConfig.scala`
- Modify: `src/main/scala/ai/starlake/semantic/SemanticExportCmd.scala`
- Test: `src/test/scala/ai/starlake/semantic/SemanticExportCmdSpec.scala` (create)

**Interfaces:**
- Consumes: existing `SemanticExportCmd.parse(args: Seq[String]): Option[SemanticExportConfig]`.
- Produces: `SemanticExportConfig` gains `connection: Option[String] = None` (4th field, before `reportFormat`). Task 5 reads `config.connection`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/semantic/SemanticExportCmdSpec.scala`:

```scala
package ai.starlake.semantic

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SemanticExportCmdSpec extends AnyFlatSpec with Matchers {

  "semantic-export parser" should "accept the lookml format and connection flag" in {
    val config = SemanticExportCmd.parse(
      Seq("--format", "lookml", "--connection", "analytics_wh", "--model", "ecommerce")
    )
    config shouldBe Some(
      SemanticExportConfig(
        format = "lookml",
        model = Some("ecommerce"),
        connection = Some("analytics_wh")
      )
    )
  }

  it should "keep accepting the ossie format and reject unknown formats" in {
    SemanticExportCmd.parse(Seq("--format", "ossie")) shouldBe Some(
      SemanticExportConfig(format = "ossie")
    )
    SemanticExportCmd.parse(Seq("--format", "tableau")) shouldBe None
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly *SemanticExportCmdSpec*"`
Expected: compile FAILURE with `unknown parameter name: connection` (the config case class has no such field yet).

- [ ] **Step 3: Write the implementation**

Replace the content of `src/main/scala/ai/starlake/semantic/SemanticExportConfig.scala` with:

```scala
package ai.starlake.semantic

import ai.starlake.job.ReportFormatConfig

case class SemanticExportConfig(
  format: String = "ossie",
  model: Option[String] = None,
  output: Option[String] = None,
  connection: Option[String] = None,
  reportFormat: Option[String] = None
) extends ReportFormatConfig
```

In `src/main/scala/ai/starlake/semantic/SemanticExportCmd.scala`:

1. Replace `pageDescription` with:

```scala
  override def pageDescription: String =
    "Export semantic models from metadata/semantic to a vendor-neutral interchange format (Apache Ossie) or a LookML project."
```

2. In `pageKeywords`, add two entries after `"apache ossie",`:

```scala
      "lookml",
      "looker",
```

3. Replace the `builder.note(...)` block with:

```scala
      builder.note(
        """
          |Export the semantic models stored in metadata/semantic/ to another semantic
          |format. Supported formats: ossie (Apache Ossie, incubating, formerly Open
          |Semantic Interchange) and lookml (a Looker project: one view file per table
          |plus a model file with explores).
          |
          |For ossie, fields, primary keys, relationships, and metrics are mapped to
          |their Ossie equivalents; Starlake-specific attributes with no Ossie
          |counterpart (filters, sample values, verified query SQL, join types...) are
          |preserved in custom_extensions blocks under the STARLAKE vendor name so no
          |information is lost.
          |
          |For lookml, dimensions, time dimensions, facts and metrics become LookML
          |dimensions, dimension_groups and measures; relationships become explores
          |with joins. --connection sets the Looker connection name in the model file
          |(defaults to the project's connectionRef).
          |
          |example: starlake semantic-export
          |         --format lookml
          |         --model ecommerce_analytics
          |         --connection analytics_wh
          |         --output /tmp/lookml-models""".stripMargin
      ),
```

4. Replace the `--format` option definition with:

```scala
      builder
        .opt[String]("format")
        .action((x, c) => c.copy(format = x))
        .validate(x =>
          if (Set("ossie", "lookml").contains(x)) builder.success
          else builder.failure(s"Unsupported format '$x'. Supported formats: ossie, lookml")
        )
        .text("Target format: ossie (default) or lookml")
        .optional(),
```

5. After the `--output` option (before `reportFormatOption`), add:

```scala
      builder
        .opt[String]("connection")
        .action((x, c) => c.copy(connection = Some(x)))
        .text(
          "lookml only: Looker connection name written to the model file. Defaults to the project's connectionRef"
        )
        .optional(),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly *SemanticExportCmdSpec*"`
Expected: PASS, 2 tests succeeded.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/SemanticExportConfig.scala src/main/scala/ai/starlake/semantic/SemanticExportCmd.scala src/test/scala/ai/starlake/semantic/SemanticExportCmdSpec.scala
git commit -m "feat(semantic): accept lookml format and --connection flag in semantic-export

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Job dispatch and end-to-end export

**Files:**
- Modify: `src/main/scala/ai/starlake/semantic/SemanticExportJob.scala:59-64` (the `selected.foreach` write loop)
- Test: `src/test/scala/ai/starlake/semantic/LookMLExportSpec.scala` (create)

**Interfaces:**
- Consumes: `LookMLConverter.convert(modelName, model, connection)` from Task 2/3; `config.connection` from Task 4; `settings.appConfig.connectionRef: String`.
- Produces: `starlake semantic-export --format lookml` writes `export/lookml/<modelName>/*.lkml`. Ossie behavior unchanged.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/ai/starlake/semantic/LookMLExportSpec.scala` (same integration style as `SemanticExportSpec`, reusing the sample model YAML):

```scala
package ai.starlake.semantic

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

class LookMLExportSpec extends TestHelper {

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
      |        unique: true
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
      |        unique: true
      |    primary_key:
      |      columns: [customer_id]
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |    join_type: left_outer
      |    relationship_type: many_to_one
      |""".stripMargin

  "semantic-export --format lookml" should "write a LookML project per model" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(
        SemanticExportConfig(format = "lookml", connection = Some("analytics_wh"))
      ).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }

      val outDir = new Path(DatasetArea.semantic, "export/lookml/ecommerce_analytics")
      val model = storage.read(new Path(outDir, "ecommerce_analytics.model.lkml"))
      model should include("""connection: "analytics_wh"""")
      model should include("explore: orders {")
      model should include("sql_on: ${orders.customer_id} = ${customers.customer_id} ;;")

      val orders = storage.read(new Path(outDir, "orders.view.lkml"))
      orders should include("sql_table_name: ANALYTICS_DB.ECOMMERCE.ORDERS ;;")
      orders should include("primary_key: yes")
      orders should include("dimension_group: order_date {")
      orders should include("measure: avg_order_value {")

      val customers = storage.read(new Path(outDir, "customers.view.lkml"))
      customers should include("view: customers {")
    }
  }

  it should "default the connection to the project's connectionRef" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(SemanticExportConfig(format = "lookml")).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }

      val model = storage.read(
        new Path(
          DatasetArea.semantic,
          "export/lookml/ecommerce_analytics/ecommerce_analytics.model.lkml"
        )
      )
      model should include(s"""connection: "${settings.appConfig.connectionRef}"""")
    }
  }

  it should "leave the ossie export untouched" in {
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
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly *LookMLExportSpec*"`
Expected: FAIL. The first two tests fail reading the model file (the job still writes `<name>.ossie.yaml` for every format, so `export/lookml/ecommerce_analytics/ecommerce_analytics.model.lkml` does not exist). The ossie test PASSES.

- [ ] **Step 3: Write the implementation**

In `src/main/scala/ai/starlake/semantic/SemanticExportJob.scala`, replace the write loop:

```scala
    selected.foreach { case (path, _, name, node) =>
      val ossie = OssieConverter.convert(name, node)
      val target = new Path(outputDir, s"$name.ossie.yaml")
      storage.write(YamlSerde.mapper.writeValueAsString(ossie), target)
      logger.info(s"Exported semantic model '$name' ($path) to $target")
    }
```

with:

```scala
    selected.foreach { case (path, _, name, node) =>
      config.format match {
        case "lookml" =>
          val connection = config.connection.getOrElse(settings.appConfig.connectionRef)
          val modelDir = new Path(outputDir, name)
          storage.mkdirs(modelDir)
          LookMLConverter.convert(name, node, connection).foreach { case (relativePath, content) =>
            val target = new Path(modelDir, relativePath)
            storage.write(content, target)
            logger.info(s"Exported semantic model '$name' ($path) to $target")
          }
        case _ =>
          val ossie = OssieConverter.convert(name, node)
          val target = new Path(outputDir, s"$name.ossie.yaml")
          storage.write(YamlSerde.mapper.writeValueAsString(ossie), target)
          logger.info(s"Exported semantic model '$name' ($path) to $target")
      }
    }
```

Also update the class scaladoc first line in the same file from:

```scala
/** Exports semantic models stored in metadata/semantic/ to the Apache Ossie (incubating)
  * interchange format.
```

to:

```scala
/** Exports semantic models stored in metadata/semantic/ to the Apache Ossie (incubating)
  * interchange format or to a LookML project.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly *LookMLExportSpec*"`
Expected: PASS, 3 tests succeeded.

- [ ] **Step 5: Run the full semantic test suite (regression)**

Run: `sbt "testOnly *SemanticExportSpec* *LookMLConverterSpec* *SemanticExportCmdSpec* *LookMLExportSpec*"`
Expected: PASS, all suites green (ossie behavior unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/ai/starlake/semantic/SemanticExportJob.scala src/test/scala/ai/starlake/semantic/LookMLExportSpec.scala
git commit -m "feat(semantic): export semantic models as LookML projects

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
