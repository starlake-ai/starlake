package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.job.Main
import better.files.File
import org.scalatest.BeforeAndAfterAll

class AutoTaskDependenciesSpec extends TestHelper with BeforeAndAfterAll {

  val starbakeDir = File("/Users/hayssams/git/starbake")
  logger.info(starbakeDir.pathAsString)
  setEnv("SL_ROOT", starbakeDir.pathAsString)
  setEnv("SL_METADATA", starbakeDir.pathAsString)

  override def beforeAll(): Unit = {}

  override def afterAll(): Unit = {}

  "Dependency Generation" should "succeed" in {
    Main.main(
      Array("dependencies", "--task", "Products.TopSellingProfitableProducts")
    )
  }
  "Job GraphViz Generation" should "succeed" in {
    Main.main(
      Array("jobs2gv", "--task", "Products.TopSellingProfitableProducts")
    )
  }
}
