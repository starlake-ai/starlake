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
