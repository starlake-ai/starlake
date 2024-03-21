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

  "Autoload" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      assert(new Main().run(Array("autoload", "--clean")))
    }
  }

  "Infer Schema" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      copyFilesToIncomingDir(sampleDataDir)
      assert(
        new Main().run(
          Array(
            "infer-schema",
            "--input",
            s"$localDir/incoming/starbake",
            "--clean"
          )
        )
      )
    }
  }

  "Import files" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(new Main().run(Array("import")))
    }
  }

  "Load files" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(new Main().run(Array("load")))
    }
  }
  "Transform revenue" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(new Main().run(Array("transform", "--name", "kpi.revenue_summary")))
    }
  }

  "Transform product" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(new Main().run(Array("transform", "--name", "kpi.product_summary")))
    }
  }

  "Transform order" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(new Main().run(Array("transform", "--name", "kpi.order_summary")))
    }
  }

  "Transform lineage" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(
        new Main().run(
          Array("lineage", "--svg", "--tasks", "kpi.order_summary", "--output", "lineage.svg")
        )
      )
    }
  }

  "Transform recursive" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "DUCKDB") {
      assert(new Main().run(Array("transform", "--recursive", "--name", "kpi.order_summary")))
    }
  }

}
