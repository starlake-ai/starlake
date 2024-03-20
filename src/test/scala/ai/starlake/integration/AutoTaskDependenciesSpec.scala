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
        val result = new Main().run(
          Array("transform", "--recursive", "--name", "kpi.order_summary")
        )
        assert(result)
      }
    }
  }

  "sample test" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT"     -> localDir.pathAsString,
        "SL_METADATA" -> localDir.pathAsString
      ) {
        assert(
          new Main().run(
            Array("acl-dependencies", "--all")
          )
        )
      }
    }
  }
  "Dependency Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString
      ) {
        assert(
          new Main().run(
            Array("lineage", "--viz", "--all")
          )
        )
      }
    }
  }

  "Relations Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        assert(
          new Main().run(
            Array("table-dependencies")
          )
        )
      }
    }
  }

  "Job GraphViz Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "true").toBoolean) {
      withEnvs(
        "SL_ROOT" -> localDir.pathAsString /* , "SL_METADATA" -> starbakeDir.pathAsString */
      ) {
        assert(
          new Main().run(
            Array(
              "lineage",
              "--print",
              "--viz",
              "--tasks",
              "kpi.order_summary"
            )
          )
        )
      }
    }
  }
}
