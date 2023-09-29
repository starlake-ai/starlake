package ai.starlake.integration

import ai.starlake.job.Main

class DagIntegrationSpec extends BigQueryIntegrationSpecBase {
  "All Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "all"
    ) {
      new WithSettings() {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)

        Main.main(
          Array("dag-generate", "--clean")
        )
      }
    }
  }

  "Domain Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "domain"
    ) {
      new WithSettings() {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)

        Main.main(
          Array("dag-generate")
        )
      }
    }
  }

  "Domain / Table Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "domain_table"
    ) {
      new WithSettings() {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)

        Main.main(
          Array("dag-generate")
        )
      }
    }
  }

  "Schedule Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule"
    ) {
      new WithSettings() {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)

        Main.main(
          Array("dag-generate")
        )
      }
    }
  }

  "Schedule / Domain Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule_domain"
    ) {
      new WithSettings() {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)

        Main.main(
          Array("dag-generate")
        )
      }
    }
  }
  "Schedule / Domain / Table Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> quickstartDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule_domain_table"
    ) {
      new WithSettings() {
        clearDataDirectories()
        incomingDir.copyToDirectory(quickstartDir)

        Main.main(
          Array("dag-generate")
        )
      }
    }
  }
}
