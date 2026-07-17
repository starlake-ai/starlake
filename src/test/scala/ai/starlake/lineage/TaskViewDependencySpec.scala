package ai.starlake.lineage

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.config.Settings.latestSchemaVersion
import ai.starlake.job.transform.AutoTask
import ai.starlake.schema.model.*
import org.apache.hadoop.fs.Path

class TaskViewDependencySpec extends TestHelper {

  "TaskViewDependency.dependencies" should "emit table name in sink and schedule in cron for TABLE-type entries" in {
    new WithSettings() {
      // Deploy a LOAD domain 'sales' with two tables:
      // - 'orders' carries a table-level schedule
      // - 'customers' has no schedule at all
      val domainConfigPath = new Path(DatasetArea.load, "sales/_config.sl.yml")
      storageHandler.mkdirs(domainConfigPath.getParent)
      storageHandler.write(
        s"""---
           |version: 1
           |load:
           |  name: "sales"
           |  metadata:
           |    directory: "$starlakeTestRoot/incoming/sales"
           |""".stripMargin,
        domainConfigPath
      )
      storageHandler.write(
        """---
          |version: 1
          |table:
          |  name: "orders"
          |  pattern: "orders.*.csv"
          |  metadata:
          |    format: "DSV"
          |    withHeader: true
          |    separator: ","
          |    schedule: "0 2 * * *"
          |  attributes:
          |    - name: "id"
          |      type: "string"
          |      required: false
          |""".stripMargin,
        new Path(DatasetArea.load, "sales/orders.sl.yml")
      )
      storageHandler.write(
        """---
          |version: 1
          |table:
          |  name: "customers"
          |  pattern: "customers.*.csv"
          |  metadata:
          |    format: "DSV"
          |    withHeader: true
          |    separator: ","
          |  attributes:
          |    - name: "id"
          |      type: "string"
          |      required: false
          |""".stripMargin,
        new Path(DatasetArea.load, "sales/customers.sl.yml")
      )

      // Transform task selecting from both load tables so its parents resolve
      // as TABLE-type dependencies (they exist as domain tables, not as tasks).
      val revenueTask = AutoTaskInfo(
        name = "revenue",
        sql = Some(
          "SELECT o.id FROM sales.orders o JOIN sales.customers c ON o.id = c.id"
        ),
        database = None,
        domain = "kpi",
        table = "revenue",
        python = None,
        writeStrategy = Some(WriteStrategy.Overwrite)
      )
      val revenueTaskDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TaskDesc(latestSchemaVersion, revenueTask))
      val revenueTaskPath = new Path(starlakeMetadataPath + "/transform/kpi/revenue.sl.yml")
      storageHandler.mkdirs(revenueTaskPath.getParent)
      storageHandler.write(revenueTaskDef, revenueTaskPath)

      val transformConfigDef = mapper
        .writer()
        .withAttribute(classOf[Settings], settings)
        .writeValueAsString(TransformDesc(latestSchemaVersion, AutoJobInfo("", Nil)))
      storageHandler.write(
        transformConfigDef,
        new Path(starlakeMetadataPath + "/transform/kpi/_config.sl.yml")
      )

      val schemaHandler = settings.schemaHandler()
      val tasks = AutoTask.unauthenticatedTasks(true)(settings, storageHandler, schemaHandler)
      val deps = TaskViewDependency.dependencies(tasks)(settings, schemaHandler)

      val tableDeps = deps.filter(_.typ == TaskViewDependency.TABLE_TYPE)
      tableDeps.map(_.name) should contain theSameElementsAs List(
        "sales.orders",
        "sales.customers"
      )

      // Scenario 1: table with a schedule -> sink = table name, cron = schedule
      val ordersDep = tableDeps
        .find(_.name == "sales.orders")
        .getOrElse(fail("expected a TABLE-type dependency named 'sales.orders'"))
      ordersDep.sink shouldBe Some("sales.orders")
      ordersDep.cron shouldBe Some("0 2 * * *")

      // Scenario 2: table without a schedule -> sink = table name, cron = "None"
      val customersDep = tableDeps
        .find(_.name == "sales.customers")
        .getOrElse(fail("expected a TABLE-type dependency named 'sales.customers'"))
      customersDep.sink shouldBe Some("sales.customers")
      customersDep.cron shouldBe Some("None")

      // Scenario 4 (non-regression): TASK-type entries keep their own sink/cron
      val taskDeps = deps.filter(_.typ == TaskViewDependency.TASK_TYPE)
      taskDeps should not be empty
      taskDeps.foreach { dep =>
        dep.name shouldBe "kpi.revenue"
        dep.sink shouldBe Some("kpi.revenue")
        dep.cron shouldBe Some("None")
      }
    }
  }
}
