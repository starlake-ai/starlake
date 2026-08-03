package ai.starlake.semantic

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class SemanticExportSpec extends TestHelper {

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

  "semantic-export" should "convert a Snowflake-style model to Ossie" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(SemanticExportConfig()).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }

      val exported =
        storage.read(new Path(DatasetArea.semantic, "export/ossie/ecommerce_analytics.ossie.yaml"))
      val root = YamlSerde.mapper.readTree(exported)

      root.path("version").asText() shouldBe OssieConverter.SpecVersion
      val sm = root.path("semantic_model").get(0)
      sm.path("name").asText() shouldBe "ecommerce_analytics"

      // datasets
      val datasets = sm.path("datasets").elements().asScala.toList
      datasets.map(_.path("name").asText()) shouldBe List("orders", "customers")
      val orders = datasets.head
      orders.path("source").asText() shouldBe "ANALYTICS_DB.ECOMMERCE.ORDERS"
      orders.path("primary_key").get(0).asText() shouldBe "order_id"
      orders.path("unique_keys").get(0).get(0).asText() shouldBe "order_id"

      // fields: 2 dimensions + 1 time dimension + 1 fact
      val fields = orders.path("fields").elements().asScala.toList
      fields.map(_.path("name").asText()) shouldBe
        List("order_id", "order_status", "order_date", "order_total")
      val status = fields(1)
      status.path("datatype").asText() shouldBe "String"
      status.path("dimension").path("is_time").asBoolean() shouldBe false
      status.path("ai_context").path("synonyms").get(0).asText() shouldBe "status"
      status
        .path("expression")
        .path("dialects")
        .get(0)
        .path("expression")
        .asText() shouldBe "ORDER_STATUS"
      fields(2).path("dimension").path("is_time").asBoolean() shouldBe true
      fields(2).path("datatype").asText() shouldBe "Date"
      // facts carry no dimension block
      fields(3).has("dimension") shouldBe false

      // relationship
      val rel = sm.path("relationships").get(0)
      rel.path("from").asText() shouldBe "orders"
      rel.path("to").asText() shouldBe "customers"
      rel.path("from_columns").get(0).asText() shouldBe "customer_id"
      rel.path("to_columns").get(0).asText() shouldBe "customer_id"

      // metrics: table-level metric hoisted to model level + model-level metric
      val metrics = sm.path("metrics").elements().asScala.toList
      metrics.map(_.path("name").asText()) shouldBe List("avg_order_value", "customer_count")

      // ai examples from verified queries; SQL preserved in custom_extensions
      sm.path("ai_context")
        .path("examples")
        .get(0)
        .asText() shouldBe "Who are the top 10 customers by revenue?"
      val ext = sm.path("custom_extensions").get(0)
      ext.path("vendor_name").asText() shouldBe "STARLAKE"
      ext.path("data").asText() should include("top_customers")
    }
  }

  it should "filter by model name and fail on unknown models" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(SemanticExportConfig(model = Some("ecommerce"))).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }
      storage.exists(
        new Path(DatasetArea.semantic, "export/ossie/ecommerce_analytics.ossie.yaml")
      ) shouldBe true

      val unknown = new SemanticExportJob(SemanticExportConfig(model = Some("nope"))).run()
      unknown.isFailure shouldBe true
    }
  }
}
