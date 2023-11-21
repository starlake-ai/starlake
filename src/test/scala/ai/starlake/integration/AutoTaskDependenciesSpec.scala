package ai.starlake.integration

import ai.starlake.job.Main

class AutoTaskDependenciesSpec extends IntegrationTestBase {

  val samplesDir = starlakeDir / "samples"
  logger.info(starlakeDir.pathAsString)
  val starbakeDir = samplesDir / "starbake"
  logger.info(starbakeDir.pathAsString)

  "Recursive Transform" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ROOT" -> starbakeDir.pathAsString
      ) {
        Main.main(
          Array("transform", "--recursive", "--name", "Products.TopSellingProfitableProducts")
        )
      }
    }
  }

  "sample test" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ROOT"     -> starbakeDir.pathAsString,
        "SL_METADATA" -> starbakeDir.pathAsString
      ) {
        Main.main(
          Array("acl-dependencies", "--all")
        )
      }
    }
  }
  "Dependency Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ROOT" -> starbakeDir.pathAsString
      ) {
        Main.main(
          Array("task-dependencies", "--viz", "--all")
        )
      }
    }
  }

  "Relations Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ROOT" -> starbakeDir.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        Main.main(
          Array("table-dependencies")
        )
      }
    }
  }

  "Job GraphViz Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ROOT" -> starbakeDir.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        Main.main(
          Array(
            "task-dependencies",
            "--print",
            "--tasks",
            "Products.TopSellingProfitableProducts"
          )
        )
      }
    }
  }
}
