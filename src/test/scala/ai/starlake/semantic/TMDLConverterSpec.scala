package ai.starlake.semantic

import ai.starlake.config.ConnectionInfo
import ai.starlake.schema.model.ConnectionType
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
      TMDLConverter
        .convert("composite", YamlSerde.mapper.readTree(yaml), None)
        .toMap
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
      TMDLConverter
        .convert("unknowns", YamlSerde.mapper.readTree(yaml), None)
        .toMap
        .apply("tables/t.tmdl")
    table.split("\n").count(_ == "\t\tdataType: string") shouldBe 2
  }

  private def pg: Option[ConnectionInfo] = Some(
    ConnectionInfo(
      `type` = ConnectionType.JDBC,
      options = Map("url" -> "jdbc:postgresql://myhost:5432/mydb")
    )
  )

  "partitions" should "emit an import-mode native query with expr AS name projections" in {
    val orders =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), pg)
        .toMap
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
    val table = TMDLConverter
      .convert("bare", YamlSerde.mapper.readTree(yaml), pg)
      .toMap
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
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), sf)
        .toMap
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
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), bq)
        .toMap
        .apply("tables/orders.tmdl")
    orders should include(
      "\t\t\t\tSource = GoogleBigQuery.Database([BillingProject=\"my-project\"]),"
    )
  }

  it should "fall back to a generic TODO source for missing or unmapped connections" in {
    val none =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), None)
        .toMap
        .apply("tables/orders.tmdl")
    none should include("\t\t\t\t// TODO Starlake: set the connector for your warehouse")
    none should include("\t\t\t\tSource = Sql.Database(\"SERVER_TODO\", \"DATABASE_TODO\"),")

    val duck = Some(
      ConnectionInfo(`type` = ConnectionType.JDBC, options = Map("url" -> "jdbc:duckdb:/tmp/db"))
    )
    val d =
      TMDLConverter
        .convert("ecommerce_analytics", YamlSerde.mapper.readTree(modelYaml), duck)
        .toMap
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
    val table = TMDLConverter
      .convert("esc", YamlSerde.mapper.readTree(yaml), pg)
      .toMap
      .apply("tables/t.tmdl")
    table should include("SELECT TRIM(\"\"COL\"\") AS trimmed FROM t")
  }
}
