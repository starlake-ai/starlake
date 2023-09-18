package ai.starlake.integration

import ai.starlake.job.Main

class DagIntegrationSpec extends BigQueryIntegrationSpecBase {
  "All Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_DAG_REF", "all")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)

    Main.main(
      Array("dag-generate", "--clean")
    )
  }

  "Domain Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_DAG_REF", "domain")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)

    Main.main(
      Array("dag-generate")
    )
  }

  "Domain / Table Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_DAG_REF", "domain_table")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)

    Main.main(
      Array("dag-generate")
    )
  }

  "Schedule Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_DAG_REF", "schedule")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)

    Main.main(
      Array("dag-generate")
    )
  }

  "Schedule / Domain Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_DAG_REF", "schedule_domain")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)

    Main.main(
      Array("dag-generate")
    )
  }
  "Schedule / Domain / Table Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    setEnv("SL_DAG_REF", "schedule_domain_table")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)

    Main.main(
      Array("dag-generate")
    )
  }
}
