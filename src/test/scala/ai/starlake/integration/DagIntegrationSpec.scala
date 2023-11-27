package ai.starlake.integration

import ai.starlake.job.Main

class DagIntegrationSpec extends BigQueryIntegrationSpecBase {
  "All Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "all"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate", "--clean")
      )
    }
  }

  "Domain Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "domain"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate")
      )
    }
  }

  "Domain / Table Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "domain_table"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate")
      )
    }
  }

  "Schedule Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate")
      )
    }
  }

  "Schedule / Domain Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule_domain"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate")
      )
    }
  }
  "Schedule / Domain / Table Dag generation" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_ENV"                      -> "LOCAL",
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_DAG_REF"                  -> "schedule_domain_table"
    ) {
      clearDataDirectories()
      sampleDataDir.copyToDirectory(localDir)

      Main.main(
        Array("dag-generate")
      )
    }
  }
}
