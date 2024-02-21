package ai.starlake.integration

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.job.Main

class LoadLocalIntegrationSpec extends IntegrationTestBase with TestHelper {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override val incomingDir = localDir / "incoming"
  override def sampleDataDir = localDir / "sample-data"
  implicit val settings: Settings = Settings(Settings.referenceConfig)
  "Import / Load / Transform Local" should "succeed" in {
    sparkSession.sql("drop table if exists sales.orders")
    sparkSession.sql("drop table if exists sales.customers")
    sparkSession.sql("drop table if exists hr.sellers")
    sparkSession.sql("drop table if exists hr.flat_locations")
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_ENV"                      -> "LOCAL"
    ) {
      cleanup()
      copyFilesToIncomingDir(sampleDataDir)
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
      val ordersCount = sparkSession.sql("select * from sales.orders").count
      assert(ordersCount == 3)
      val sellersCount = sparkSession.sql("select * from hr.sellers").count
      assert(sellersCount == 1)

      val customers =
        sparkSession.sql("select name2 from sales.customers").collect().map(_.getString(0))
      val customersCount = customers.size
      assert(customersCount == 25)
      assert(customers.contains("RemoveLater"))
      val locationsCount = sparkSession.sql("select * from hr.flat_locations").count
      assert(locationsCount == 2)
    }
  }
  "Import / Load / Transform Local 2" should "succeed" in {
    withEnvs(
      "SL_ROOT" -> localDir.pathAsString,
      "SL_ENV"  -> "LOCAL"
    ) {
      val sampleDataDir2 = localDir / "sample-data2"
      copyFilesToIncomingDir(sampleDataDir2)
      Main.main(
        Array("import")
      )
      Main.main(
        Array("load")
      )
      val ordersCount = sparkSession.sql("select * from sales.orders").count
      assert(ordersCount == 6)
      val sellersCount = sparkSession.sql("select * from hr.sellers").count
      assert(sellersCount == 2)
      val customers =
        sparkSession.sql("select name2 from sales.customers").collect().map(_.getString(0))
      val customersCount = customers.size
      assert(customersCount == 25)
      assert(!customers.contains("RemoveLater"))
      assert(customers.contains("Bama"))
      val locationsCount = sparkSession.sql("select * from hr.flat_locations").count
      assert(locationsCount == 2)
    }
  }
}
