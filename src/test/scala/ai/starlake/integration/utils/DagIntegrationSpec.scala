package ai.starlake.integration.utils

import ai.starlake.integration.BigQueryIntegrationSpecBase
import ai.starlake.job.Main

class DagIntegrationSpec extends BigQueryIntegrationSpecBase {
  "All Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "all"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate", "--clean")
        )
      )
    }
  }

  "Domain Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "domain"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate")
        )
      )
    }
  }

  "Domain / Table Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "domain_table"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate")
        )
      )
    }
  }

  "Schedule Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate")
        )
      )
    }
  }

  "Schedule / Domain Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule_domain"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate")
        )
      )
    }
  }
  "Schedule / Domain / Table Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> theSampleFolder.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule_domain_table"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)

      assert(
        new Main().run(
          Array("dag-generate")
        )
      )
    }
  }
}
