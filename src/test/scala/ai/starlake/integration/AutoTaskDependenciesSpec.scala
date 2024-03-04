package ai.starlake.integration

import ai.starlake.job.Main

class AutoTaskDependenciesSpec extends IntegrationTestBase {

  val samplesDir = starlakeDir / "samples"
  logger.info(starlakeDir.pathAsString)
  override val localDir = samplesDir / "starbake"
  logger.info(localDir.pathAsString)

  "Recursive Transform" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString
      ) {
        // FIXME: if there is failure during domain load, it is a success. How should we fix it ? To reproduce, just remove version from load/starbake/_config.sl.yml
        // FIXME: it is a success even if there is exception during transform
        Main.main(
          Array("transform", "--recursive", "--name", "Products.TopSellingProfitableProducts")
        )
      }
    }
  }

  "sample test" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT"     -> localDir.pathAsString,
        "SL_METADATA" -> localDir.pathAsString
      ) {
        Main.main(
          Array("acl-dependencies", "--all")
        )
      }
    }
  }
  "Dependency Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString
      ) {
        Main.main(
          Array("lineage", "--viz", "--all")
        )
      }
    }
  }

  "Relations Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        Main.main(
          Array("table-dependencies")
        )
      }
    }
  }

  "Job GraphViz Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        Main.main(
          Array(
            "lineage",
            "--print",
            "--viz",
            "--tasks",
            "Products.TopSellingProfitableProducts"
          )
        )
      }
    }
  }
}
