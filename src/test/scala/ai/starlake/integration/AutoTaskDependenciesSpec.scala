package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.job.Main
import ai.starlake.job.transform.TaskViewDependency
import ai.starlake.utils.Utils
import better.files.File
import org.scalatest.BeforeAndAfterAll

class AutoTaskDependenciesSpec extends TestHelper with BeforeAndAfterAll {

  val starbakeDir = File(System.getProperty("user.home") + "/git/starbake")
  logger.info(starbakeDir.pathAsString)
  setEnv("SL_ROOT", starbakeDir.pathAsString)
  setEnv("SL_METADATA", starbakeDir.pathAsString)

  override def beforeAll(): Unit = {}

  override def afterAll(): Unit = {}

  "Sezrialize Task View Dependency" should "succeed" in {
    val data = List(TaskViewDependency("a", "b", "c", "d", "e", None))
    Utils.newJsonMapper().writeValueAsString(data)
  }

  "Recursive Transform" should "succeed" in {
    Main.main(
      Array("transform", "--recursive", "--name", "Products.TopSellingProfitableProducts")
    )
  }

  "Dependency Generation" should "succeed" in {
    Main.main(
      Array("dependencies", "--viz")
    )
  }
  "Relations Generation" should "succeed" in {
    Main.main(
      Array("yml2gv", "--domains")
    )
  }
  "Job GraphViz Generation" should "succeed" in {
    Main.main(
      Array("dependencies", "--task", "Products.TopSellingProfitableProducts")
    )
  }
}
