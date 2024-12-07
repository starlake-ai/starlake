package ai.starlake.integration.load

import ai.starlake.TestHelper
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import org.apache.spark.sql.DataFrame

import scala.reflect.io.Directory

class InferSchemaXMLIntegrationSpec extends IntegrationTestBase with TestHelper {
  override def templates = starlakeDir / "samples"
  override def localDir = templates / "spark"
  override val incomingDir = localDir / "incoming"
  override def sampleDataDir = localDir / "sample-data"

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
  override def afterEach(): Unit = {
    super.afterEach()
  }

  private def dropTables: DataFrame = {
    sparkSession.sql("drop table if exists sales.customers")
    sparkSession.sql("drop table if exists sales.categories")
    sparkSession.sql("drop table if exists sales.products")
    sparkSession.sql("drop table if exists sales.orders")
    sparkSession.sql("drop table if exists hr.sellers")
    sparkSession.sql("drop table if exists hr.flat_locations")
  }

  "Import / Load / Transform Local" should "succeed" in {
    withEnvs(
      "SL_ROOT"                     -> localDir.pathAsString,
      "SL_INTERNAL_SUBSTITUTE_VARS" -> "true",
      "SL_ENV"                      -> "LOCAL"
    ) {
      new Directory(new java.io.File(settings.appConfig.datasets)).deleteRecursively()

      dropTables
      copyFilesToIncomingDir(sampleDataDir)
      assert(
        new Main().run(
          Array("import")
        )
      )
      assert(new Main().run(Array("load")))
      val ordersCount = sparkSession.sql("select * from sales.orders").count
      assert(ordersCount == 3)
      val sellersCount = sparkSession.sql("select * from hr.sellers").count
      assert(sellersCount == 1)

      val customers =
        sparkSession.sql("select name2 from sales.customers").collect().map(_.getString(0))
      val customersCount = customers.length
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
      dropTables
      List(localDir / "sample-data", localDir / "sample-data2").foreach { sampleDataDir =>
        copyFilesToIncomingDir(sampleDataDir)
        assert(
          new Main().run(
            Array("import")
          )
        )
        assert(new Main().run(Array("load")))
      }
      val orders = sparkSession.sql("select * from sales.orders")
      orders.show(false)
      val ordersCount = orders.count
      assert(ordersCount == 6)

      val sellers = sparkSession.sql("select * from hr.sellers")
      sellers.show(false)
      val sellersCount = sellers.count
      assert(sellersCount == 2)

      val customers = sparkSession.sql("select name2 from sales.customers")
      customers.show(false)
      val customersCount = customers.count()
      assert(customersCount == 25)
      val custCollection = customers.collect().map(_.getString(0))
      assert(!custCollection.contains("RemoveLater"))
      assert(custCollection.contains("Bama"))

      val locations = sparkSession.sql("select * from hr.flat_locations")
      locations.show(false)
      val locationsCount = locations.count
      assert(locationsCount == 2)

      val cats = sparkSession.sql("select * from sales.categories")
      cats.show(false)
      val catsCount = cats.count
      assert(catsCount == 5)
    }
  }
}
