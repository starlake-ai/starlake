package ai.starlake.integration.starbake

import ai.starlake.config.Settings
import ai.starlake.integration.IntegrationTestBase

class StarbakeDiffAttributesSpecextends extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  logger.info(theSampleFolder.pathAsString)
  "Diff Attributes" should "succeed" in {
    withEnvs("SL_ROOT" -> theLinageFolder.pathAsString, "SL_ENV" -> "DUCKDB") {
      val currentEnv = Option(System.getenv("SL_ENV"))
      implicit val settings: Settings =
        Settings(Settings.referenceConfig, currentEnv, None, None, None)
      val taskFullName = "starbake_analytics.customer_purchase_history"
      val res = settings.schemaHandler().syncPreviewSqlWithDb(taskFullName, None, None)
      res.foreach { case (tableAttribute, attributeStatus) =>
        println(
          s"Table Attribute: ${tableAttribute.name}: ${tableAttribute.`type`}, Status: $attributeStatus"
        )

      }
      /*
      settings.schemaHandler().taskByName(taskName) match {
        case Success(task) =>
          val list: List[(TableAttribute, AttributeStatus)] =
            task.diffSqlAttributesWithYaml(None)

          val updatedTask = task.updateAttributes(list.map(_._1))
          YamlSerde.serializeToPath(new Path("/tmp/updated_task.yaml"), updatedTask)(
            settings.storageHandler()
          )
          logger.info(s"Diff SQL attributes with YAML for task $taskName: $list")
        case Failure(exception) =>
          logger.error("Failed to get task", exception)
          throw exception

      }
       */
    }
  }
}
