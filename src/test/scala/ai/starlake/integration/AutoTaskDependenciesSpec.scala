package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.job.Main
import better.files.File
import org.scalatest.BeforeAndAfterAll

class AutoTaskDependenciesSpec extends TestHelper with BeforeAndAfterAll {

  val starbakeDir = File(System.getProperty("user.home") + "/git/starbake")
  logger.info(starbakeDir.pathAsString)

  override def beforeAll(): Unit = {}

  override def afterAll(): Unit = {}

  "Recursive Transform" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> starbakeDir.pathAsString, "SL_METADATA" -> starbakeDir.pathAsString) {
        Main.main(
          Array("transform", "--recursive", "--name", "Products.TopSellingProfitableProducts")
        )
      }
    }
  }

  "sample test" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs(
        "SL_ROOT"     -> "/Users/hayssams/git/public/starlake/samples/local/quickstart",
        "SL_METADATA" -> "/Users/hayssams/git/public/starlake/samples/local/quickstart/metadata"
      ) {
        Main.main(
          Array("acl-dependencies")
        )
      }
    }
  }
  "Dependency Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> starbakeDir.pathAsString, "SL_METADATA" -> starbakeDir.pathAsString) {
        Main.main(
          Array("task-dependencies", "--viz")
        )
      }
    }
  }

  "Relations Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> starbakeDir.pathAsString, "SL_METADATA" -> starbakeDir.pathAsString) {
        Main.main(
          Array("table-dependencies")
        )
      }
    }
  }

  "Job GraphViz Generation" should "succeed" in {
    if (sys.env.getOrElse("SL_LOCAL_TEST", "false").toBoolean) {
      withEnvs("SL_ROOT" -> starbakeDir.pathAsString, "SL_METADATA" -> starbakeDir.pathAsString) {
        Main.main(
          Array(
            "task-dependencies",
            "--viz",
            "--tasks",
            "Products.TopSellingProducts,Products.MostProfitableProducts"
          )
        )
      }
    }
  }
}
