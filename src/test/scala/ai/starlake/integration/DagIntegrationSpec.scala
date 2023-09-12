package ai.starlake.integration

import ai.starlake.job.Main

class DagIntegrationSpec extends IntegrationSpecBase {
  "All Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)
    val loadDir = quickstartDir / "metadata" / "load"

    setEnv("SL_DAG_REF", "all")
    Main.main(
      Array("dag-generate")
    )
  }
  "Domain Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)
    val loadDir = quickstartDir / "metadata" / "load"

    setEnv("SL_DAG_REF", "domain")
    Main.main(
      Array("dag-generate")
    )
  }
  "Domain / Table Dag generation" should "succeed" in {
    setEnv("SL_ENV", "LOCAL")
    setEnv("SL_INTERNAL_SUBSTITUTE_VARS", "true")
    clearDataDirectories()
    incomingDir.copyToDirectory(quickstartDir)
    val loadDir = quickstartDir / "metadata" / "load"

    setEnv("SL_DAG_REF", "domain_table")
    Main.main(
      Array("dag-generate")
    )
  }
}
