package ai.starlake.integration

import ai.starlake.job.Main
import better.files.File

class AclDependenciesSpec extends IntegrationTestBase {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
  val incomingDir = localDir / "incoming"
  val quickstartDir: File = localDir / "quickstart"
  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")

  "All ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
      Main.main(
        Array("acl-dependencies")
      )
    }
  }

  "Some ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
      Main.main(
        Array("acl-dependencies", "--grantees", "user:me@me.com,user:you@you.com")
      )
    }
  }
}
