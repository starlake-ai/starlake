package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.extract.JdbcDbUtils
import com.typesafe.config.{Config, ConfigFactory}

class DuckDbNativeJsonLoadSpec extends TestHelper {

  lazy val duckDbConfiguration: Config = {
    val config = ConfigFactory.parseString(
      s"""
         |connectionRef: "test-duckdb"
         |connections.test-duckdb {
         |    type = "jdbc"
         |    options {
         |      "url": "jdbc:duckdb:${starlakeTestRoot}/test_json_native.db"
         |      "driver": "org.duckdb.DuckDBDriver"
         |    }
         |}
         |""".stripMargin
    )
    config.withFallback(super.testConfiguration)
  }

  new WithSettings(duckDbConfiguration) {

    private def queryDuckDb[T](sql: String)(f: java.sql.ResultSet => T): T = {
      val options = settings.appConfig.connections("test-duckdb").options
      JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), options) { conn =>
        val rs = conn.createStatement().executeQuery(sql)
        f(rs)
      }
    }

    // Regression for https://github.com/starlake-ai/starlake/issues/1701: the nested JSON
    // branch whose first attribute is not a variant built the INSERT SQL but never executed
    // it, silently loading 0 rows while reporting success.
    "Native DuckDB load of a nested JSON file with a non-variant first attribute" should
    "actually insert the rows" in {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/jsonduck/jsonduck.sl.yml",
        datasetDomainName = "jsonduck",
        sourceDatasetPathName = "/sample/jsonduck/XJSONDUCKTBL"
      ) {
        cleanMetadata
        deliverSourceDomain()
        deliverSourceTable(
          "jsonduck",
          "/sample/jsonduck/user_jsonduck.sl.yml",
          Some("user.sl.yml")
        )

        val result = loadPending
        result.isSuccess shouldBe true

        val rows = queryDuckDb(
          """SELECT id, payload->'a'->>'b' AS b FROM jsonduck.user ORDER BY id"""
        ) { rs =>
          val buf = scala.collection.mutable.ListBuffer[(Long, String)]()
          while (rs.next()) {
            buf += ((rs.getLong("id"), rs.getString("b")))
          }
          buf.toList
        }

        rows shouldBe List((1L, "x"), (2L, "y"))
      }
    }
  }
}
