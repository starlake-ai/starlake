package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.job.Main
import better.files.File
import org.scalatest.BeforeAndAfterAll

class AclDependenciesSpec extends TestHelper with BeforeAndAfterAll {

  val starlakeDir = File(".")
  logger.info(starlakeDir.pathAsString)
  val localDir = starlakeDir / "samples" / "local"
  val incomingDir = localDir / "incoming"
  val quickstartDir: File = localDir / "quickstart"
  val directoriesToClear = List("incoming", "audit", "datasets", "diagrams")

  override def beforeAll(): Unit = {}

  override def afterAll(): Unit = {}

  "All ACL Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
        Main.main(
          Array("acl-dependencies")
        )
      }
    }
  }

  "Some ACL Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> quickstartDir.pathAsString) {
        Main.main(
          Array("acl-dependencies", "--grantees", "user:me@me.com,user:you@you.com")
        )
      }
    }
  }
}
