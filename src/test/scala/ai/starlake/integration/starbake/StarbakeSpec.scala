package ai.starlake.integration.starbake

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class StarbakeSpec extends IntegrationTestBase {

  logger.info(starlakeDir.pathAsString)

  override protected def cleanup(): Unit = {
    // do not cleanup between tests
  }

  override def localDir = starlakeDir / "samples" / "starbake"
  override def sampleDataDir = localDir / "sample-data"
  logger.info(localDir.pathAsString)
  "Infer Schema" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      Main.main(
        Array(
          "infer-schema",
          "--input",
          s"$localDir/incoming/starbake",
          "--clean"
        )
      )
    }
  }

  "Import files" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(Array("import"))
    }
  }

  "Load files" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(Array("load"))
    }
  }
  "Transform revenue" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(Array("transform", "--name", "kpi.revenue_summary"))
    }
  }

  "Transform product" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(Array("transform", "--name", "kpi.product_summary"))
    }
  }

  "Transform order" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(Array("transform", "--name", "kpi.order_summary"))
    }
  }

  "Transform lineage" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(
        Array("lineage", "--svg", "--tasks", "kpi.order_summary", "--output", "lineage.svg")
      )
    }
  }

  "Transform recursive" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      Main.main(Array("transform", "--recursive", "--name", "kpi.order_summary"))
    }
  }

}